<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Repository;

use Whirlwind\Domain\Repository\ResultFactoryInterface;
use Whirlwind\Domain\Repository\ResultInterface;

class ElasticResultFactory implements ResultFactoryInterface
{
    public function create(array $data): ResultInterface
    {
        return new ElasticResult($data);
    }
}
