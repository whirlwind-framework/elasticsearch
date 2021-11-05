<?php declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence;

class BulkCommand
{
    protected $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }
}
