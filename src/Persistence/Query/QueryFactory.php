<?php declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence\Query;

class QueryFactory
{
    public function create(): Query
    {
        return new Query();
    }
}
