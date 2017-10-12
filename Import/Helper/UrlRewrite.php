<?php

namespace Pimgento\Import\Helper;

use \Pimgento\Entities\Model\Entities;
use \Magento\Framework\App\Helper\AbstractHelper;
use \Magento\Framework\App\Helper\Context;
use \Magento\Framework\DB\Adapter\AdapterInterface;
use \Zend_Db_Expr as Expr;

class UrlRewrite extends AbstractHelper
{
    /**
     * @var Entities
     */
    protected $_entities;

    /**
     * PHP Constructor
     *
     * @param Context  $context
     * @param Entities $entities
     */
    public function __construct(
        Context $context,
        Entities $entities
    ) {
        $this->_entities = $entities;

        parent::__construct($context);
    }

    /**
     * Create temporary table for url rewrite
     *
     * @return void
     */
    public function createUrlTmpTable()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tableRewrite = $resource->getTable('tmp_pimgento_rewrite');

        $this->dropUrlRewriteTmpTable();

        $table = $connection->newTable($tableRewrite);
        $table->addColumn('entity_id', \Magento\Framework\DB\Ddl\Table::TYPE_INTEGER, 10, ['unsigned' => true]);
        $table->addColumn('entity_type', \Magento\Framework\DB\Ddl\Table::TYPE_TEXT, 255, []);
        $table->addColumn('store_id', \Magento\Framework\DB\Ddl\Table::TYPE_SMALLINT, 5, ['unsigned' => true]);
        $table->addColumn('old_request_path', \Magento\Framework\DB\Ddl\Table::TYPE_TEXT, 255, ['unsigned' => true]);
        $table->addColumn('request_path', \Magento\Framework\DB\Ddl\Table::TYPE_TEXT, 255, ['unsigned' => true]);
        $table->addColumn('target_path', \Magento\Framework\DB\Ddl\Table::TYPE_TEXT, 255, ['unsigned' => true]);
        $table->addColumn('url_rewrite_id', \Magento\Framework\DB\Ddl\Table::TYPE_INTEGER, 10, ['unsigned' => true]);
        $table->addIndex(
            $tableRewrite.'entity_id',
            ['entity_id'],
            ['type' => AdapterInterface::INDEX_TYPE_INDEX]
        );
        $table->addIndex(
            $tableRewrite.'store_id',
            ['entity_id'],
            ['type' => AdapterInterface::INDEX_TYPE_INDEX]
        );
        $connection->createTable($table);
    }

    /**
     * Drop temporary table
     */
    public function dropUrlRewriteTmpTable()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tableRewrite = $resource->getTable('tmp_pimgento_rewrite');

        $connection->dropTable($tableRewrite);
    }

    /**
     * Perform url rewriting
     *
     * @param string $code
     * @param int    $storeId
     * @param string $column
     * @param string $urlSuffix
     *
     * @return void
     */
    public function rewriteUrls($code, $storeId, $column, $urlSuffix)
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable           = $this->_entities->getTableName($code);
        $tmpUrlRewriteTable = $resource->getTable('tmp_pimgento_rewrite');
        $urlRewriteTable    = $resource->getTable('url_rewrite');

        if ($code === 'variant'){
            $code = 'product';
        }

        $targetPathExpr     = new Expr('CONCAT("catalog/' . $code . '/view/id/", `_entity_id`)');


        // Fill temporary url table
        $values = [
            'entity_id'        => 't._entity_id',
            'entity_type'      => new Expr('"' . $code . '"'),
            'store_id'         => new Expr($storeId),
            'request_path'     => new Expr('CONCAT(`t`.`' . $column . '`, "'.$urlSuffix.'")'),
            'target_path'      => $targetPathExpr,
            'old_request_path' => 'u.request_path',
            'url_rewrite_id'   => 'u.url_rewrite_id',
        ];

        $select = $connection->select()
            ->from(
                ['t' => $tmpTable],
                $values
            )
            ->joinLeft(
                array('u' => $urlRewriteTable),
                't._entity_id = u.entity_id 
                AND u.entity_type = "' . $code . '" 
                AND u.redirect_type = 0
                AND u.target_path = ' . $targetPathExpr . '
                AND u.store_id = ' . $storeId,
                array()
            )
            ->where('`t`.`' . $column . '` <> ""')
            ->where('`t`.`' . $column . '` NOT LIKE "%' . \Pimgento\Entities\Model\ResourceModel\Entities::IGNORE_VALUE . '%"');


        $connection->query(
            $query = $connection->insertFromSelect(
                $select,
                $tmpUrlRewriteTable,
                array_keys($values),
                AdapterInterface::INSERT_ON_DUPLICATE
            )
        );

        // Clean system generated urls (301, category paths)
        $this->_cleanSystemUrlsBeforeInsertion($storeId);

        // Get values for update and insert in url_rewrite table
        $values = array(
            'url_rewrite_id'   => 'url_rewrite_id',
            'entity_type'      => new Expr('"' . $code . '"'),
            'entity_id'        => 'entity_id',
            'request_path'     => 'request_path',
            'target_path'      => 'target_path',
            'redirect_type'    => new Expr('0'),
            'store_id'         => new Expr($storeId),
            'is_autogenerated' => new Expr('1'),
        );

        // Perform update on url_rewrite_table
        $rewrite = $connection->select()
            ->from($tmpUrlRewriteTable, $values)
            ->where('`request_path` <> `old_request_path` AND `url_rewrite_id` IS NOT NULL AND `store_id` = ' . $storeId);

        $this->_insertInUrlRewriteTable($rewrite, array_keys($values), AdapterInterface::INSERT_ON_DUPLICATE);

        // Perform insert on url_rewrite_table
        unset($values['url_rewrite_id']);
        $rewrite = $connection->select()
            ->from($tmpUrlRewriteTable, $values)
            ->where('`url_rewrite_id` IS NULL AND `store_id` = ' . $storeId);

        $this->_insertInUrlRewriteTable($rewrite, array_keys($values), AdapterInterface::INSERT_IGNORE);
    }

    /**
     * URL rewrite cleaning
     *
     * @param int $storeId
     * @return void
     */
    protected function _cleanSystemUrlsBeforeInsertion($storeId)
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpUrlRewriteTable = $resource->getTable('tmp_pimgento_rewrite');
        $urlRewriteTable    = $resource->getTable('url_rewrite');

        $select = $connection->select()
            ->from(
                ['t' => $tmpUrlRewriteTable],
                ['entity_id', 'url_rewrite_id', 'entity_type']
            )
            ->where('`request_path` <> `old_request_path` AND `url_rewrite_id` IS NOT NULL AND store_id =' . $storeId);
        $urls = $connection->fetchAll($select);

        $entityIdsToDelete = [];
        $urlIdsToKeep = [];
        foreach ($urls as $url) {
            // Marking type of url rewrites entity types to delete
            $entityIdsToDelete[$url['entity_type']][] = (int)$url['entity_id'];
            $urlIdsToKeep[] = (int)$url['url_rewrite_id'];
        }

        if (count($entityIdsToDelete)) {
            foreach ($entityIdsToDelete as $entityType => $subEntityIdsToDelete) {
                $connection->delete(
                    $urlRewriteTable,
                    'entity_id IN (' . implode(',', $subEntityIdsToDelete) . ')
                     AND entity_type = "' . $entityType . '"
                     AND url_rewrite_id NOT IN (' . implode(',', $subEntityIdsToDelete) . ')
                     AND store_id = ' . $storeId
                );

                // Delete urls associated to the products in that category
                if ($entityType == 'category') {
                    foreach ($subEntityIdsToDelete as $catEntity) {
                        $connection->delete(
                            $urlRewriteTable,
                            'target_path LIKE ("%category/' . $catEntity . '") AND store_id = ' . $storeId
                        );
                    }
                }
            }
        }
    }

    /**
     * DB Insertion
     *
     * @param \Magento\Framework\DB\Select $select
     * @param array                        $columns
     * @param int                          $insertionType
     *
     * @return void
     */
    protected function _insertInUrlRewriteTable($select, $columns, $insertionType)
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $urlRewriteTable = $resource->getTable('url_rewrite');

        $connection->query(
            $connection->insertFromSelect(
                $select,
                $urlRewriteTable,
                $columns,
                $insertionType
            )
        );
    }
}
