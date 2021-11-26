<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence\Query;

use Whirlwind\ElasticSearch\Persistence\Connection;

class QueryFactory
{
    public function create(Connection $connection): Query
    {
        return new Query($connection);
    }
}
