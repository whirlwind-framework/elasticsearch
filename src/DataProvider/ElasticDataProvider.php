<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\DataProvider;

use Whirlwind\ElasticSearch\DataProvider\Dto\AggregationDto;
use Whirlwind\ElasticSearch\Repository\ElasticResult;
use Whirlwind\Infrastructure\DataProvider\DataProvider;
use Whirlwind\Infrastructure\DataProvider\Pagination;

class ElasticDataProvider extends DataProvider
{
    /**
     * @var array
     */
    protected array $aggregations = [];

    protected function loadData()
    {
        $offset = ($this->page - 1) * $this->limit;
        $result = $this->repository->findAll($this->conditions, $this->sortFields, $this->limit, $offset);

        if (!$result instanceof ElasticResult) {
            throw new \InvalidArgumentException(
                \sprintf('Expected \'%s\', got \'%s\'', ElasticResult::class, \get_class($result))
            );
        }

        $this->models = \iterator_to_array($result);
        $this->pagination = new Pagination($result->getTotal(), $this->limit, $this->page);

        foreach ($result->getAggregations() as $name => $value) {
            $this->aggregations[] = new AggregationDto([
                'name' => $name,
                'value' => $value,
            ]);
        }
    }

    /**
     * @return array
     */
    public function getAggregations(): array
    {
        if (!$this->dataLoaded) {
            $this->loadData();
        }

        return $this->aggregations;
    }

    #[\ReturnTypeWillChange]
    public function jsonSerialize()
    {
        return [
            'items' => $this->models,
            'pagination' => $this->pagination,
            'aggregations' => $this->aggregations,
        ];
    }
}
