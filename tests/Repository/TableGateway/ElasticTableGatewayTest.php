<?php

declare(strict_types=1);

namespace Test\Repository\TableGateway;

use DG\BypassFinals;
use PHPUnit\Framework\MockObject\MockObject;
use Whirlwind\ElasticSearch\Persistence\Command;
use Whirlwind\ElasticSearch\Persistence\ConditionBuilder\ConditionBuilder;
use Whirlwind\ElasticSearch\Persistence\Connection;
use Whirlwind\ElasticSearch\Persistence\Query\Query;
use Whirlwind\ElasticSearch\Persistence\Query\QueryFactory;
use Whirlwind\ElasticSearch\Repository\TableGateway\ElasticTableGateway;
use PHPUnit\Framework\TestCase;

class ElasticTableGatewayTest extends TestCase
{
    private MockObject $connection;
    private MockObject $queryFactory;
    private MockObject $conditionBuilder;
    private string $collectionName = 'collection';
    private string $documentType = '';

    private ElasticTableGateway $tableGateway;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->connection = $this->createMock(Connection::class);
        $this->queryFactory = $this->createMock(QueryFactory::class);
        $this->conditionBuilder = $this->createMock(ConditionBuilder::class);

        $this->tableGateway = new ElasticTableGateway(
            $this->connection,
            $this->queryFactory,
            $this->conditionBuilder,
            $this->collectionName,
            $this->documentType
        );
    }

    public function testQueryAllAggregation()
    {
        $aggs = [
            'price_min' => [
                'min' => [
                    'field' => 'some_field',
                ],
            ],
            'price_reversed' => [
                'reverse_nested' => [],
            ],
        ];

        $this->conditionBuilder->expects(self::once())
            ->method('build')
            ->willReturn([]);

        $query = $this->createMock(Query::class);
        $this->queryFactory->expects(self::once())
            ->method('create')
            ->with(self::identicalTo($this->connection))
            ->willReturn($query);

        $query->expects(self::once())
            ->method('from')
            ->with(self::identicalTo($this->collectionName))
            ->willReturnSelf();

        $query->expects(self::once())
            ->method('where')
            ->willReturnSelf();

        $query->expects(self::exactly(2))
            ->method('addAggregate')
            ->withConsecutive(
                [self::equalTo('price_min'), self::identicalTo($aggs['price_min'])],
                [self::equalTo('price_reversed'), self::callback(static function ($value) {
                    return $value['reverse_nested'] instanceof \stdClass;
                })]
            )
            ->willReturnSelf();

        $command = $this->createMock(Command::class);
        $query->expects(self::once())
            ->method('createCommand')
            ->willReturn($command);

        $command->expects(self::once())
            ->method('search')
            ->willReturn([]);

        $this->tableGateway->queryAll([
            'aggregations' => $aggs,
        ]);
    }
}
