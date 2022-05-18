<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Repository;

use Whirlwind\Infrastructure\Repository\Result;

class ElasticResult extends Result
{
    protected int $total;
    protected array $aggregations;
    /**
     * @param array $meta
     */
    public function __construct(array $meta)
    {
        parent::__construct($meta['hits']['hits'] ?? []);

        $this->total = $meta['hits']['total']['value'] ?? 0;
        $this->aggregations = $meta['aggregations'] ?? [];
    }

    public function getTotal(): int
    {
        return $this->total;
    }

    public function getAggregations(): array
    {
        return $this->aggregations;
    }
}
