<?php

namespace Pimgento\Variant\Observer;

use Magento\Framework\Event\ObserverInterface;
use Pimgento\Import\Observer\AbstractAddImportObserver;

class AddPimgentoImportObserver extends AbstractAddImportObserver implements ObserverInterface
{
    /**
     * Get the import code
     *
     * @return string
     */
    protected function getImportCode()
    {
        return 'variant';
    }

    /**
     * Get the import name
     *
     * @return string
     */
    protected function getImportName()
    {
        return __('Variants');
    }

    /**
     * Get the default import classname
     *
     * @return string
     */
    protected function getImportDefaultClassname()
    {
        return '\Pimgento\Variant\Model\Factory\Import';
    }

    /**
     * Get the sort order
     *
     * @return int
     */
    protected function getImportSortOrder()
    {
        return 50;
    }

    /**
     * get the steps definition
     *
     * @return array
     */
    protected function getStepsDefinition()
    {
        $stepsBefore = array(
            array(
                'comment' => __('Create temporary table'),
                'method'  => 'createTable',
            ),
            array(
                'comment' => __('Fill temporary table'),
                'method'  => 'insertData',
            ),
            array(
                'comment' => __('Add product required data'),
                'method'  => 'addRequiredData',
            ),
            array(
                'comment' => __('Match code with Magento ID'),
                'method'  => 'matchEntity',
            ),
            array(
                'comment' => __('Clean up variants'),
                'method'  => 'removeColumns',
            ),
            array(
                'comment' => __('Variants data enrichment'),
                'method'  => 'addColumns',
            ),
            array(
                'comment' => __('Update column values for options'),
                'method'  => 'updateOption',
            ),
            array(
                'comment' => __('Fill variants data'),
                'method'  => 'updateData',
            )
        );

        $stepsAfter = array(
            array(
                'comment' => __('Set values to attributes'),
                'method'  => 'setValues',
            ),
            array(
                'comment' => __('Set products to websites'),
                'method'  => 'setWebsites',
            ),
            array(
                'comment' => __('Set products to categories'),
                'method'  => 'setCategories',
            ),
            array(
                'comment' => __('Set Url Rewrite'),
                'method'  => 'setUrlRewrite',
            ),
            array(
                'comment' => __('Import media files'),
                'method'  => 'importMedia',
            ),
            array(
                'comment' => __('Drop temporary table'),
                'method'  => 'dropTable',
            ),
            array(
                'comment' => __('Clean cache'),
                'method'  => 'cleanCache',
            )
        );

        return array_merge(
            $stepsBefore,
            $this->getAdditionnalSteps(),
            $stepsAfter
        );
    }
}
