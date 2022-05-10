<?php

declare(strict_types=1);

namespace Test\Repository;

use DG\BypassFinals;
use Whirlwind\ElasticSearch\Repository\ElasticResult;
use PHPUnit\Framework\TestCase;

class ElasticResultTest extends TestCase
{
    private array $meta = [
        'hits' => [
            'total' => [
                'value' => 1,
            ],
            'hits' => [
                [
                    'id' => 'test',
                ],
            ],
        ],
        'aggregations' => [
            'price_min' => [
                'value' => 10000,
            ],
        ],
    ];

    private ElasticResult $result;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->result = new ElasticResult($this->meta);
    }

    public function testGetTotal()
    {
        self::assertEquals($this->meta['hits']['total']['value'], $this->result->getTotal());
    }

    public function testGetAggregations()
    {
        self::assertEquals($this->meta['aggregations'], $this->result->getAggregations());
    }

    public function testCreateOnEmptyArray()
    {
        $result = new ElasticResult([]);

        self::assertEquals(0, $result->getTotal());
        self::assertEmpty($result->getAggregations());
    }
}
