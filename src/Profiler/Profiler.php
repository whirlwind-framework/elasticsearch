<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Profiler;

use Whirlwind\Infrastructure\Profiler\AbstractProfiler;
use Whirlwind\Infrastructure\Profiler\TimerInterface;
use Whirlwind\ElasticSearch\Persistence\Connection;

class Profiler extends AbstractProfiler
{
    /**
     * @var TimerInterface[]
     */
    protected $timers = [];

    protected $connection;

    protected $collectionName;

    public function __construct(Connection $connection, string $collectionName, array $defaultTags = [])
    {
        \register_shutdown_function([$this, 'flush']);
        $this->connection = $connection;
        $this->collectionName = $collectionName;

        if (!$this->connection->createCommand()->indexExists($this->collectionName)) {
            $this->connection->createCommand()->createIndex(
                $this->collectionName,
                [
                    'mappings' => [
                        'properties' => [
                            'startMilliseconds' => [
                                'type' => 'date',
                                'format' => 'epoch_millis'
                            ],
                            'timer' => ['type' => 'keyword'],
                        ],
                    ],
                    'settings' => [
                        'number_of_shards' => 1,
                        'number_of_replicas' => 1,
                    ]
                ]
            );
        }

        parent::__construct($defaultTags);
    }

    public function flush(): void
    {
        foreach ($this->timers as $timer) {
            /** @var Timer $timer */
            $data = [
                'timer' => $timer->getName(),
                'start' => $timer->getStart(),
                'startMilliseconds' => $timer->getStartMilliseconds(),
                'duration' => $timer->getTime(),
                'tags' => $timer->getTags()
            ];
            $connection = $this->connection;
            try {
                $connection->post(\sprintf('%s/_doc', $this->collectionName), [], \json_encode($data));
            } catch (\Exception $e) {
                //@TODO need some logging?
            }
        }
        $this->timers = [];
    }

    public function startTimer(string $timerName, array $tags = []): TimerInterface
    {
        $timer = new Timer($timerName, $this->prepareTags($tags));
        $this->timers[$timerName] = $timer;
        return $timer;
    }

    public function stopTimer(TimerInterface $timer): void
    {
        /** @var Timer $timer */
        $timer->stop();
    }

    public function stopTimerByName(string $timerName): void
    {
        if (isset($this->timers[$timerName])) {
            $this->stopTimer($this->timers[$timerName]);
        }
    }
}
