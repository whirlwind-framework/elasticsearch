<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\DataProvider\Dto;

use Whirlwind\Domain\Dto\Dto;

class AggregationDto extends Dto
{
    protected $name;
    protected $value;

    /**
     * @return string
     */
    public function getName(): string
    {
        return (string) $this->name;
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }
}
