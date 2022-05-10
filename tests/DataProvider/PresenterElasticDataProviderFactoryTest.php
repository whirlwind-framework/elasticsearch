<?php

declare(strict_types=1);

namespace Test\DataProvider;

use DG\BypassFinals;
use Whirlwind\Domain\Presenter\PresenterInterface;
use Whirlwind\Domain\Repository\RepositoryInterface;
use Whirlwind\ElasticSearch\DataProvider\PresenterElasticDataProvider;
use Whirlwind\ElasticSearch\DataProvider\PresenterElasticDataProviderFactory;
use PHPUnit\Framework\TestCase;

class PresenterElasticDataProviderFactoryTest extends TestCase
{
    private PresenterElasticDataProviderFactory $factory;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->factory = new PresenterElasticDataProviderFactory();
    }

    public function testCreate()
    {
        $presenter = $this->createMock(PresenterInterface::class);
        $repository = $this->createMock(RepositoryInterface::class);

        $actual = $this->factory->create($presenter, $repository);
        self::assertInstanceOf(PresenterElasticDataProvider::class, $actual);
    }
}
