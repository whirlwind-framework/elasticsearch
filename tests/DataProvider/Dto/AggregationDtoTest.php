<?php

declare(strict_types=1);

namespace Test\DataProvider\Dto;

use DG\BypassFinals;
use Whirlwind\ElasticSearch\DataProvider\Dto\AggregationDto;
use PHPUnit\Framework\TestCase;

class AggregationDtoTest extends TestCase
{
    private array $data = [
        'name' => 1,
        'value' => [
            'value' => 10000,
        ],
    ];
    private AggregationDto $dto;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->dto = new AggregationDto($this->data);
    }

    public function testGetName()
    {
        self::assertSame((string) $this->data['name'], $this->dto->getName());
    }

    public function testGetValue()
    {
        self::assertSame($this->data['value'], $this->dto->getValue());
    }
}
