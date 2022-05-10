<?php

declare(strict_types=1);

namespace Test\DataProvider;

use DG\BypassFinals;
use Whirlwind\Domain\Repository\RepositoryInterface;
use Whirlwind\ElasticSearch\DataProvider\ElasticDataProvider;
use Whirlwind\ElasticSearch\DataProvider\ElasticDataProviderFactory;
use PHPUnit\Framework\TestCase;

class ElasticDataProviderFactoryTest extends TestCase
{
    private ElasticDataProviderFactory $factory;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->factory = new ElasticDataProviderFactory();
    }

    public function testCreate()
    {
        $repository = $this->createMock(RepositoryInterface::class);
        $actual = $this->factory->create($repository);

        self::assertInstanceOf(ElasticDataProvider::class, $actual);
    }
}
