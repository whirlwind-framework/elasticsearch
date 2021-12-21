<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Profiler;

use Whirlwind\Infrastructure\Profiler\TimerInterface;

class Timer implements TimerInterface
{
    protected $name;

    protected $start;

    protected $startMilliseconds;

    protected $time = null;

    protected $stoped;

    /**
     * @var array
     */
    protected $tags;

    public function __construct(string $name, array $tags = [])
    {
        $this->name = $name;
        $microtime = \microtime(true);
        $this->start = $microtime;
        $this->startMilliseconds = (int)($microtime * 1000);
        $this->stoped = false;
        $this->tags = $tags;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getStart(): float
    {
        return $this->start;
    }

    public function stop(): void
    {
        if (false === $this->stoped) {
            $this->time = \microtime(true) - $this->start;
            $this->stoped = true;
        }
    }

    public function getTime(): float
    {
        if (false === $this->stoped) {
            $this->stop();
        }
        return $this->time;
    }

    /**
     * @return array
     */
    public function getTags(): array
    {
        return $this->tags;
    }

    /**
     * @param string $name
     * @param $value
     * @return void
     */
    public function addTag(string $name, $value): void
    {
        if (!\array_key_exists($name, $this->tags)) {
            $this->tags[$name] = $value;
        }
    }

    /**
     * @return int
     */
    public function getStartMilliseconds(): int
    {
        return $this->startMilliseconds;
    }
}
