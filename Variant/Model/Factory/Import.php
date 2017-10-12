<?php

namespace Pimgento\Variant\Model\Factory;

use \Pimgento\Import\Model\Factory;
use \Pimgento\Entities\Model\Entities;
use \Pimgento\Import\Helper\Config as helperConfig;
use \Magento\Framework\Event\ManagerInterface;
use \Magento\Framework\App\Cache\TypeListInterface;
use \Pimgento\Import\Helper\UrlRewrite as urlRewriteHelper;
use \Magento\Eav\Model\Entity\Attribute\SetFactory;
use \Magento\Framework\Module\Manager as moduleManager;
use \Magento\Framework\App\Config\ScopeConfigInterface as scopeConfig;
use \Pimgento\Variant\Model\Factory\Import\Media;
use \Pimgento\Variant\Helper\Media as mediaHelper;
use \Pimgento\Product\Helper\Config as productHelper;
use \Zend_Db_Expr as Expr;
use \Exception;

class Import extends Factory
{

    /**
     * @var Entities
     */
    protected $_entities;

    /**
     * @var TypeListInterface
     */
    protected $_cacheTypeList;

    /**
     * @var Media $_media
     */
    protected $_media;

    /**
     * @var \Pimgento\Product\Helper\Media
     */
    protected $_mediaHelper;

    /**
     * @var \Pimgento\Product\Helper\Config
     */
    protected $_productHelper;

    /**
     * list of allowed type_id that can be imported
     * @var string[]
     */
    protected $_allowedTypeId = ['configurable'];

    /**
     * @var urlRewriteHelper
     */
    protected $_urlRewriteHelper;

    /**
     * Import constructor.
     * @param Entities $entities
     * @param helperConfig $helperConfig
     * @param moduleManager $moduleManager
     * @param scopeConfig $scopeConfig
     * @param ManagerInterface $eventManager
     * @param TypeListInterface $cacheTypeList
     * @param mediaHelper $mediaHelper
     * @param Media $media
     * @param array $data
     */
    public function __construct(
        Entities $entities,
        helperConfig $helperConfig,
        moduleManager $moduleManager,
        scopeConfig $scopeConfig,
        ManagerInterface $eventManager,
        TypeListInterface $cacheTypeList,
        mediaHelper $mediaHelper,
        productHelper $productHelper,
        urlRewriteHelper $urlRewriteHelper,
        Media $media,
        array $data = []
    )
    {
        parent::__construct($helperConfig, $eventManager, $moduleManager, $scopeConfig, $data);
        $this->_entities = $entities;
        $this->_cacheTypeList = $cacheTypeList;
        $this->_mediaHelper = $mediaHelper;
        $this->_productHelper = $productHelper;
        $this->_urlRewriteHelper = $urlRewriteHelper;
        $this->_media = $media;
    }

    /**
     * Create temporary table
     */
    public function createTable()
    {
        $file = $this->getFileFullPath();

        if (!is_file($file)) {
            $this->setContinue(false);
            $this->setStatus(false);
            $this->setMessage($this->getFileNotFoundErrorMessage());
        } else {
            $this->_entities->createTmpTableFromFile($file, $this->getCode(), array('code', 'axis'));
        }
    }

    /**
     * Insert data into temporary table
     */
    public function insertData()
    {
        $file = $this->getFileFullPath();

        $count = $this->_entities->insertDataFromFile($file, $this->getCode());

        $this->setMessage(
            __('%1 line(s) found', $count)
        );
    }

    /**
     * Remove columns from variant table
     */
    public function removeColumns()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();

        $except = array('code', 'axis');

        $variantTable = $resource->getTable('pimgento_variant');

        $columns = array_keys($connection->describeTable($variantTable));

        foreach ($columns as $column) {
            if (in_array($column, $except)) {
                continue;
            }

            $connection->dropColumn($variantTable, $column);
        }
    }

    /**
     * Add columns to variant table
     */
    public function addColumns()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $except = array('code', 'axis', 'type', '_entity_id', '_is_new');

        $variantTable = $resource->getTable('pimgento_variant');

        $columns = array_keys($connection->describeTable($tmpTable));

        foreach ($columns as $column) {
            if (in_array($column, $except)) {
                continue;
            }

            $connection->addColumn($variantTable, $this->_columnName($column), 'TEXT');
        }
    }

    /**
     * Add or update data in variant table
     */
    public function updateData()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $variantTable = $resource->getTable('pimgento_variant');

        $variant = $connection->query(
            $connection->select()->from($tmpTable)
        );

        $attributes = $connection->fetchPairs(
            $connection->select()->from(
                $resource->getTable('eav_attribute'), array('attribute_code', 'attribute_id')
            )
                ->where('entity_type_id = ?', 4)
        );

        $columns = array_keys($connection->describeTable($tmpTable));

        $values = [];
        $i = 0;
        $keys = [];
        while (($row = $variant->fetch())) {

            $values[$i] = [];

            foreach ($columns as $column) {

                if ($connection->tableColumnExists($variantTable, $this->_columnName($column))) {

                    $values[$i][$this->_columnName($column)] = $row[$column];


                    if ($column == 'axis') {
                        $axisAttributes = explode(',', $row['axis']);

                        $axis = array();

                        foreach ($axisAttributes as $code) {
                            if (isset($attributes[$code])) {
                                $axis[] = $attributes[$code];
                            }
                        }

                        $values[$i][$column] = join(',', $axis);
                    }

                    $keys = array_keys($values[$i]);
                }
            }
            $i++;

            /**
             * Write 500 values at a time.
             */
            if (count($values) > 500) {
                $connection->insertOnDuplicate($variantTable, $values, $keys);
                $values = [];
                $i = 0;
            }
        }

        if (count($values) > 0) {
            $connection->insertOnDuplicate($variantTable, $values, $keys);
        }
    }

    /**
     * Add required data
     */
    public function addRequiredData()
    {
        $connection = $this->_entities->getResource()->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $connection->addColumn($tmpTable, '_type_id', 'VARCHAR(255) NOT NULL DEFAULT "configurable"');
        $connection->addColumn($tmpTable, '_options_container', 'VARCHAR(255) NOT NULL DEFAULT "container2"');
        $connection->addColumn($tmpTable, '_tax_class_id', 'INT(11) NOT NULL DEFAULT 0'); // None
        $connection->addColumn($tmpTable, '_attribute_set_id', 'INT(11) NOT NULL DEFAULT "4"'); // Default
        $connection->addColumn($tmpTable, '_visibility', 'INT(11) NOT NULL DEFAULT "4"'); // catalog, search
        $connection->addColumn($tmpTable, '_status', 'INT(11) NOT NULL DEFAULT "2"'); // Disabled

        if (!$connection->tableColumnExists($tmpTable, 'url_key')) {
            $connection->addColumn($tmpTable, 'url_key', 'varchar(255) NOT NULL DEFAULT ""');
            $connection->update($tmpTable, array('url_key' => new Expr('LOWER(`code`)')));
        }

        if ($connection->tableColumnExists($tmpTable, 'enabled')) {
            $connection->update($tmpTable, array('_status' => new Expr('IF(`enabled` <> 1, 2, 1)')));
        }


        if ($connection->tableColumnExists($tmpTable, 'type_id')) {
            $types = $connection->quote($this->_allowedTypeId);
            $connection->update(
                $tmpTable,
                array(
                    '_type_id' => new Expr("IF(`type_id` IN ($types), `type_id`, 'configurable')")
                )
            );
        }

        $matches = $this->_scopeConfig->getValue('pimgento/product/attribute_mapping');

        if ($matches) {
            $matches = unserialize($matches);
            if (is_array($matches)) {
                $stores = array_merge(
                    $this->_helperConfig->getStores(array('lang')), // en_US
                    $this->_helperConfig->getStores(array('lang', 'channel_code')), // en_US-channel
                    $this->_helperConfig->getStores(array('channel_code')), // channel
                    $this->_helperConfig->getStores(array('currency')), // USD
                    $this->_helperConfig->getStores(array('channel_code', 'currency')), // channel-USD
                    $this->_helperConfig->getStores(array('lang', 'channel_code', 'currency')) // en_US-channel-USD
                );
                foreach ($matches as $match) {
                    $pimAttr = $match['pim_attribute'];
                    $magentoAttr = $match['magento_attribute'];
                    $this->_entities->copyColumn($tmpTable, $pimAttr, $magentoAttr);

                    foreach ($stores as $local => $affected) {
                        $this->_entities->copyColumn($tmpTable, $pimAttr . '-' . $local, $magentoAttr . '-' . $local);
                    }
                }

            }
        }
    }

    /**
     * Set website
     */
    public function setWebsites()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $websites = $this->_helperConfig->getStores('website_id');

        foreach ($websites as $websiteId => $affected) {
            if ($websiteId == 0) {
                continue;
            }

            $select = $connection->select()
                ->from(
                    $tmpTable,
                    array(
                        'product_id' => '_entity_id',
                        'website_id' => new Expr($websiteId)
                    )
                );
            $connection->query(
                $connection->insertFromSelect(
                    $select, $resource->getTable('catalog_product_website'), array('product_id', 'website_id'), 1
                )
            );
        }
    }

    /**
     * Set Url Rewrite
     */
    public function setUrlRewrite()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $stores = array_merge(
            $this->_helperConfig->getStores(['lang']), // en_US
            $this->_helperConfig->getStores(['lang', 'channel_code']) // en_US-channel
        );

        $this->_urlRewriteHelper->createUrlTmpTable();

        $columns = [];

        foreach ($stores as $local => $affected) {
            if ($connection->tableColumnExists($tmpTable, 'url_key-' . $local)) {
                foreach ($affected as $store) {
                    $columns[$store['store_id']] = 'url_key-' . $local;
                }
            }
        }

        if (!count($columns)) {
            foreach ($stores as $local => $affected) {
                foreach ($affected as $store) {
                    $columns[$store['store_id']] = 'url_key';
                }
            }
        }

        foreach ($columns as $store => $column) {
            if ($store == 0) {
                continue;
            }

            $duplicates = $connection->fetchCol(
                $connection->select()
                    ->from($tmpTable, [$column])
                    ->group($column)
                    ->having('COUNT(*) > 1')
            );

            foreach ($duplicates as $urlKey) {
                if ($urlKey) {
                    $connection->update(
                        $tmpTable,
                        [$column => new Expr('CONCAT(`' . $column . '`, "-", `code`)')],
                        ['`' . $column . '` = ?' => $urlKey]
                    );
                }
            }

            $this->_entities->setValues(
                $this->getCode(),
                $resource->getTable('catalog_product_entity'),
                ['url_key' => $column],
                4,
                $store,
                1
            );

            $this->_urlRewriteHelper->rewriteUrls(
                $this->getCode(),
                $store,
                $column,
                $this->_scopeConfig->getValue('catalog/seo/product_url_suffix')
            );

        }


        $this->_urlRewriteHelper->dropUrlRewriteTmpTable();
    }

    /**
     * Set categories
     */
    public function setCategories()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        if (!$connection->tableColumnExists($tmpTable, 'categories')) {
            $this->setStatus(false);
            $this->setMessage(
                __('Column categories not found')
            );
        } else {

            $select = $connection->select()
                ->from(
                    array(
                        'c' => $resource->getTable('pimgento_entities')
                    ),
                    array()
                )
                ->joinInner(
                    array('p' => $tmpTable),
                    'FIND_IN_SET(`c`.`code`, `p`.`categories`) AND `c`.`import` = "category"',
                    array(
                        'category_id' => 'c.entity_id',
                        'product_id'  => 'p._entity_id'
                    )
                )
                ->joinInner(
                    array('e' => $resource->getTable('catalog_category_entity')),
                    'c.entity_id = e.entity_id',
                    array()
                );

            $connection->query(
                $connection->insertFromSelect(
                    $select,
                    $resource->getTable('catalog_category_product'),
                    array('category_id', 'product_id'),
                    1
                )
            );

            //Remove product from old categories
            $selectToDelete = $connection->select()
                ->from(
                    array(
                        'c' => $resource->getTable('pimgento_entities')
                    ),
                    array()
                )
                ->joinInner(
                    array('p' => $tmpTable),
                    '!FIND_IN_SET(`c`.`code`, `p`.`categories`) AND `c`.`import` = "category"',
                    array(
                        'category_id' => 'c.entity_id',
                        'product_id'  => 'p._entity_id'
                    )
                )
                ->joinInner(
                    array('e' => $resource->getTable('catalog_category_entity')),
                    'c.entity_id = e.entity_id',
                    array()
                );


            $connection->delete($resource->getTable('catalog_category_product'),
                '(category_id, product_id) IN (' . $selectToDelete->assemble() . ')');
        }
    }


    /**
     * Set values to attributes
     */
    public function setValues()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $stores = array_merge(
            $this->_helperConfig->getStores(array('lang')), // en_US
            $this->_helperConfig->getStores(array('lang', 'channel_code')), // en_US-channel
            $this->_helperConfig->getStores(array('channel_code')), // channel
            $this->_helperConfig->getStores(array('currency')), // USD
            $this->_helperConfig->getStores(array('channel_code', 'currency')), // channel-USD
            $this->_helperConfig->getStores(array('lang', 'channel_code', 'currency')) // en_US-channel-USD
        );

        $columns = array_keys($connection->describeTable($tmpTable));

        $except = array(
            '_entity_id',
            '_is_new',
            '_status',
            '_type_id',
            '_options_container',
            '_tax_class_id',
            '_attribute_set_id',
            '_visibility',
            '_children',
            '_axis',
            'sku',
            'categories',
            'family',
            'groups',
            'enabled',
        );

        $values = array(
            0 => array(
                'options_container' => '_options_container',
                'tax_class_id'      => '_tax_class_id',
                'visibility'        => '_visibility',
            )
        );

        if ($connection->tableColumnExists($tmpTable, 'enabled')) {
            $values[0]['status'] = '_status';
        }

        $taxClasses = $this->_productHelper->getProductTaxClasses();
        if (count($taxClasses)) {
            foreach ($taxClasses as $storeId => $taxClassId) {
                $values[$storeId]['tax_class_id'] = new Expr($taxClassId);
            }
        }

        foreach ($columns as $column) {
            if (in_array($column, $except)) {
                continue;
            }

            if (preg_match('/-unit/', $column)) {
                continue;
            }

            $columnPrefix = explode('-', $column);
            $columnPrefix = reset($columnPrefix);

            foreach ($stores as $suffix => $affected) {
                if (preg_match('/^' . $columnPrefix . '-' . $suffix . '$/', $column)) {
                    foreach ($affected as $store) {
                        if (!isset($values[$store['store_id']])) {
                            $values[$store['store_id']] = array();
                        }
                        $values[$store['store_id']][$columnPrefix] = $column;
                    }
                }
            }

            if (!isset($values[0][$columnPrefix])) {
                $values[0][$columnPrefix] = $column;
            }
        }

        foreach($values as $storeId => $data) {
            $this->_entities->setValues(
                $this->getCode(), $resource->getTable('catalog_product_entity'), $data, 4, $storeId, 1
            );
        }

    }

    /**
     * Drop temporary table
     */
    public function dropTable()
    {
        $this->_entities->dropTable($this->getCode());
    }

    /**
     * Clean cache
     */
    public function cleanCache()
    {
        $types = array(
            \Magento\Framework\App\Cache\Type\Block::TYPE_IDENTIFIER,
            \Magento\PageCache\Model\Cache\Type::TYPE_IDENTIFIER
        );

        foreach ($types as $type) {
            $this->_cacheTypeList->cleanType($type);
        }

        $this->setMessage(
            __('Cache cleaned for: %1', join(', ', $types))
        );
    }

    /**
     * Replace column name
     *
     * @param string $column
     * @return string
     */
    protected function _columnName($column)
    {
        $matches = array(
            'label' => 'name',
        );

        foreach ($matches as $name => $replace) {
            if (preg_match('/^' . $name . '/', $column)) {
                $column = preg_replace('/^' . $name . '/', $replace, $column);
            }
        }

        return $column;
    }

    /**
     * Match code with entity
     */
    public function matchEntity()
    {
        $this->_entities->matchEntity($this->getCode(), 'code', 'catalog_product_entity', 'entity_id',null,'product');
    }

    /**
     * Replace option code by id
     */
    public function updateOption()
    {
        $resource = $this->_entities->getResource();
        $connection = $resource->getConnection();
        $tmpTable = $this->_entities->getTableName($this->getCode());

        $columns = array_keys($connection->describeTable($tmpTable));

        $except = array(
            '_entity_id',
            '_is_new',
            '_status',
            '_type_id',
            '_options_container',
            '_tax_class_id',
            '_attribute_set_id',
            '_visibility',
            '_children',
            '_axis',
            'sku',
            'categories',
            'family',
            'groups',
            'url_key',
            'enabled',
        );

        foreach ($columns as $column) {

            if (in_array($column, $except)) {
                continue;
            }

            if (preg_match('/-unit/', $column)) {
                continue;
            }

            $columnPrefix = explode('-', $column);
            $columnPrefix = reset($columnPrefix);

            if ($connection->tableColumnExists($tmpTable, $column)) {
                //get number of chars to remove from code in order to use the substring.
                $prefixL = strlen($columnPrefix . '_') + 1;

                // Sub select to increase performance versus FIND_IN_SET
                $subSelect = $connection->select()
                    ->from(
                        array('c' => $resource->getTable('pimgento_entities')),
                        array('code' => 'SUBSTRING(`c`.`code`,' . $prefixL . ')', 'entity_id' => 'c.entity_id')
                    )
                    ->where("c.code like '".$columnPrefix."_%' ")
                    ->where("c.import = ?", 'option');

                // if no option no need to continue process
                if (!$connection->query($subSelect)->rowCount()) {
                    continue;
                }
                //in case of multiselect
                $conditionJoin = "IF ( locate(',', `".$column."`) > 0 , ". "`p`.`".$column."` like ".
                    new Expr("CONCAT('%', `c1`.`code`, '%')") .", `p`.`".$column."` = `c1`.`code` )";

                $select = $connection->select()
                    ->from(
                        array('p' => $tmpTable),
                        array(
                            'code'       => 'p.code',
                            'entity_id' => 'p._entity_id'
                        )
                    )
                    ->joinInner(
                        array('c1' => new Expr('('.(string) $subSelect.')')),
                        new Expr($conditionJoin),
                        array(
                            $column => new Expr('GROUP_CONCAT(`c1`.`entity_id` SEPARATOR ",")')
                        )
                    )
                    ->group('p.code');

                $connection->query(
                    $connection->insertFromSelect($select, $tmpTable, array('code', '_entity_id', $column), 1)
                );
            }
        }
    }

    /**
     * Import the medias
     */
    public function importMedia()
    {
        $enabled = $this->_scopeConfig->getValue('pimgento/image/enabled');

        if (!$enabled) {
            $this->setMessage(
                __('Media importation is disabled (Stores > Configuration > Catalog > Pimgento > Image)')
            );
        } else {
            $this->_media->setCode($this->getCode());

            $this->_mediaHelper->initHelper(dirname($this->getFileFullPath()));


            $connection = $this->_entities->getResource()->getConnection();
            $tmpTable = $this->_entities->getTableName($this->getCode());

            $tableColumns = array_keys($connection->describeTable($tmpTable));
            $fields = $this->_mediaHelper->getFields();



            $this->_media->mediaCreateTmpTables();

            foreach ($fields as $field) {
                foreach ($field['columns'] as $position => $column) {
                    if (in_array($column, $tableColumns)) {
                        $this->_media->mediaPrepareValues($column, $field['attribute_id'], $position);
                    }
                }
            }


            $this->_media->mediaCleanValues();
            $this->_media->mediaRemoveUnknownFiles();
            $this->_media->mediaCopyFiles();
            $this->_media->mediaUpdateDataBase();
            $this->_media->mediaDropTmpTables();

        }
    }


}