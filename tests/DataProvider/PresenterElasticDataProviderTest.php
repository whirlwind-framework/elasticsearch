<?php

declare(strict_types=1);

namespace Test\DataProvider;

use DG\BypassFinals;
use PHPUnit\Framework\MockObject\MockObject;
use Whirlwind\Domain\Dto\Dto;
use Whirlwind\Domain\Presenter\PresenterInterface;
use Whirlwind\Domain\Repository\RepositoryInterface;
use Whirlwind\ElasticSearch\DataProvider\PresenterElasticDataProvider;
use PHPUnit\Framework\TestCase;
use Whirlwind\ElasticSearch\Repository\ElasticResult;

class PresenterElasticDataProviderTest extends TestCase
{
    private MockObject $presenter;
    private MockObject $repository;
    private PresenterElasticDataProvider $dataProvider;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->presenter = $this->createMock(PresenterInterface::class);
        $this->repository = $this->createMock(RepositoryInterface::class);
        $this->dataProvider = new PresenterElasticDataProvider($this->presenter, $this->repository);
    }

    public function testGetModels()
    {
        $dto = $this->createMock(Dto::class);

        $result = $this->createMock(ElasticResult::class);
        $this->repository->expects(self::once())
            ->method('findAll')
            ->willReturn($result);

        $items = [new \stdClass()];
        $this->mockIterator($result,  $items);

        $result->expects(self::once())
            ->method('getTotal')
            ->willReturn(1);

        $result->expects(self::once())
            ->method('getAggregations')
            ->willReturn([]);

        $this->presenter->expects(self::once())
            ->method('decorate')
            ->with(self::identicalTo($items[0]))
            ->willReturn($dto);

        $expected = [$dto];
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
}
