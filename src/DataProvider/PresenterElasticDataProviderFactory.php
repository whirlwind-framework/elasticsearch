<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\DataProvider;

use Whirlwind\Domain\DataProvider\DataProviderInterface;
use Whirlwind\Domain\DataProvider\PresenterDataProviderFactoryInterface;
use Whirlwind\Domain\Presenter\PresenterInterface;
use Whirlwind\Domain\Repository\RepositoryInterface;

class PresenterElasticDataProviderFactory implements PresenterDataProviderFactoryInterface
{
    /**
     * @param PresenterInterface $presenter
     * @param RepositoryInterface $repository
     * @param array $conditions
     * @param array $sortFields
     * @param int $limit
     * @param int $page
     * @return DataProviderInterface
     */
    public function create(
        PresenterInterface $presenter,
        RepositoryInterface $repository,
        array $conditions = [],
        array $sortFields = [],
        int $limit = self::DEFAULT_LIMIT,
        int $page = 0
    ): DataProviderInterface {
        return new PresenterElasticDataProvider(
            $presenter,
            $repository,
            $conditions,
            $sortFields,
            $limit,
            $page
        );
    }
}
