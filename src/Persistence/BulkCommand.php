<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence;

class BulkCommand
{
    protected $connection;

    protected $index;

    protected $type;

    protected $actions;

    protected $options;

    public function __construct(
        Connection $connection,
        $index = '',
        $type = '',
        $actions = '',
        $options = []
    ) {
        $this->connection = $connection;
        $this->index = $index;
        $this->type = $type;
        $this->actions = $actions;
        $this->options = $options;
    }

    public function execute()
    {
        if ($this->index === null && $this->type === null) {
            $endpoint = ['_bulk'];
        } elseif ($this->index !== null && $this->type === null) {
            $endpoint = [$this->index, '_bulk'];
        } elseif ($this->index !== null && $this->type !== null) {
            if ($this->connection->getDslVersion() >= 7) {
                $endpoint = [$this->index, '_bulk'];
            } else {
                $endpoint = [$this->index, $this->type, '_bulk'];
            }
        } else {
            throw new \LogicException('Invalid endpoint: if type is defined, index must be defined too.');
        }

        if (empty($this->actions)) {
            $body = '{}';
        } elseif (\is_array($this->actions)) {
            $body = '';
            foreach ($this->actions as $action) {
                $body .= \json_encode($action) . "\n";
            }
        } else {
            $body = $this->actions;
        }

        return $this->connection->post($endpoint, $this->options, $body);
    }

    public function addAction($line1, $line2 = null)
    {
        if (!\is_array($this->actions)) {
            $this->actions = [];
        }

        $this->actions[] = $line1;

        if ($line2 !== null) {
            $this->actions[] = $line2;
        }
    }

    public function addDeleteAction($id, $index = null, $type = null)
    {
        $actionData = ['_id' => $id];

        if (!empty($index)) {
            $actionData['_index'] = $index;
        }

        if (!empty($type)) {
            $actionData['_type'] = $type;
        }

        $this->addAction(['delete' => $actionData]);
    }
}
