<?php

declare(strict_types=1);

namespace Test\DataProvider;

use DG\BypassFinals;
use PHPUnit\Framework\MockObject\MockObject;
use Whirlwind\Domain\Repository\RepositoryInterface;
use Whirlwind\ElasticSearch\DataProvider\Dto\AggregationDto;
use Whirlwind\ElasticSearch\DataProvider\ElasticDataProvider;
use PHPUnit\Framework\TestCase;
use Whirlwind\ElasticSearch\Repository\ElasticResult;

class ElasticDataProviderTest extends TestCase
{
    private MockObject $repository;
    private ElasticDataProvider $dataProvider;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->repository = $this->createMock(RepositoryInterface::class);
        $this->dataProvider = new ElasticDataProvider($this->repository);
    }

    public function testGetModels()
    {
        $result = $this->createMock(ElasticResult::class);
        $this->repository->expects(self::once())
            ->method('findAll')
            ->willReturn($result);

        $expected = [new \stdClass()];
        $this->mockIterator($result,  $expected);

        $result->expects(self::once())
            ->method('getTotal')
            ->willReturn(1);

        $result->expects(self::once())
            ->method('getAggregations')
            ->willReturn([]);

        $actual = $this->dataProvider->getModels();
        self::assertSame($expected, $actual);
    }

    private function mockIterator(MockObject $iterator, array $items): void
    {
        $iterator->expects(self::any())
            ->method('current')
            ->willReturnCallback(static function () use (&$items) {
                return \current($items);
            });

        $iterator->expects(self::any())
            ->method('next')
            ->willReturnCallback(static function () use (&$items) {
                return \next($items);
            });

        $iterator->expects(self::any())
            ->method('key')
            ->willReturnCallback(static function () use (&$items) {
                return \key($items);
            });

        $iterator->expects(self::any())
            ->method('valid')
            ->willReturnCallback(static function () use (&$items) {
                return (bool) \current($items);
            });

        $iterator->expects(self::any())
            ->method('rewind')
            ->willReturnCallback(static function () use (&$items) {
                return (bool) \reset($items);
            });
    }

    public function testGetAggregations()
    {
        $result = $this->createMock(ElasticResult::class);
        $this->repository->expects(self::once())
            ->method('findAll')
            ->willReturn($result);

        $result->expects(self::once())
            ->method('getTotal')
            ->willReturn(0);

        $result->expects(self::once())
            ->method('getAggregations')
            ->willReturn([
                'price_min' => [
                    'value' => 10000,
                ]
            ]);

        $actual = $this->dataProvider->getAggregations();

        self::assertCount(1, $actual);
        self::assertInstanceOf(AggregationDto::class, $actual[0]);

        $expected = [
            'name' => 'price_min',
            'value' => [
                'value' => 10000,
            ],
        ];
        self::assertSame($expected, $actual[0]->toArray());
    }

    public function testJsonSerialize()
    {
        $actual = $this->dataProvider->jsonSerialize();
        self::assertArrayHasKey('aggregations', $actual);
    }


}
