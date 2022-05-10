<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\DataProvider;

use Whirlwind\Domain\DataProvider\DataProviderFactoryInterface;
use Whirlwind\Domain\DataProvider\DataProviderInterface;
use Whirlwind\Domain\Repository\RepositoryInterface;

class ElasticDataProviderFactory implements DataProviderFactoryInterface
{
    public function create(
        RepositoryInterface $repository,
        array $conditions = [],
        array $sortFields = [],
        int $limit = self::DEFAULT_LIMIT,
        int $page = 0
    ): DataProviderInterface {
        return new ElasticDataProvider(
            $repository,
            $conditions,
            $sortFields,
            $limit,
            $page
        );
    }
}
