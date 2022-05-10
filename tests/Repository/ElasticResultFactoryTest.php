<?php

declare(strict_types=1);

namespace Test\Repository;

use DG\BypassFinals;
use Whirlwind\ElasticSearch\Repository\ElasticResult;
use Whirlwind\ElasticSearch\Repository\ElasticResultFactory;
use PHPUnit\Framework\TestCase;

class ElasticResultFactoryTest extends TestCase
{
    private ElasticResultFactory $factory;

    protected function setUp(): void
    {
        BypassFinals::enable();
        parent::setUp();

        $this->factory = new ElasticResultFactory();
    }

    public function testCreate()
    {
        $actual = $this->factory->create([]);
        self::assertInstanceOf(ElasticResult::class, $actual);
    }
}
