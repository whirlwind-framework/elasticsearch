<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence\Query;

use Whirlwind\ElasticSearch\Persistence\Connection;

class Query extends \Whirlwind\Infrastructure\Persistence\Query
{
    protected $storedFields;

    protected $scriptFields;

    protected $source;

    protected $index;

    protected $type;

    protected $timeout;

    protected $query;

    protected $filter;

    protected $postFilter;

    protected $highlight;

    protected $aggregations = [];

    protected $stats = [];

    protected $suggest = [];

    protected $collapse = [];

    protected $minScore;

    protected $options = [];

    protected $explain;

    protected $indexBy;

    public function __construct(Connection $connection)
    {
        parent::__construct($connection);
    }

    public function createCommand()
    {
        $commandConfig = $this->connection->getQueryBuilder()->build($this);
        return $this->connection->createCommand($commandConfig);
    }

    public function all(): array
    {
        $result = $this->createCommand()->search();
        if ($result === false) {
            throw new \RuntimeException('Elasticsearch search query failed.');
        }
        if (empty($result['hits']['hits'])) {
            return [];
        }
        $rows = $result['hits']['hits'];
        return $this->populate($rows);
    }

    public function populate($rows)
    {
        if ($this->indexBy === null) {
            return $rows;
        }
        $models = [];
        foreach ($rows as $key => $row) {
            if ($this->indexBy !== null) {
                if (\is_string($this->indexBy)) {
                    $key = isset($row['fields'][$this->indexBy]) ?
                        \reset($row['fields'][$this->indexBy]) : $row['_source'][$this->indexBy];
                } else {
                    $key = \call_user_func($this->indexBy, $row);
                }
            }
            $models[$key] = $row;
        }
        return $models;
    }

    public function one()
    {
        $result = $this->connection->createCommand()->search(['size' => 1]);
        if ($result === false) {
            throw new \RuntimeException('Elasticsearch search query failed.');
        }
        if (empty($result['hits']['hits'])) {
            return false;
        }
        $record = \reset($result['hits']['hits']);

        return $record;
    }

    public function search($options = [])
    {
        $result = $this->connection->createCommand()->search($options);
        if ($result === false) {
            throw new \RuntimeException('Elasticsearch search query failed.');
        }
        if (!empty($result['hits']['hits']) && $this->indexBy !== null) {
            $rows = [];
            foreach ($result['hits']['hits'] as $key => $row) {
                if (\is_string($this->indexBy)) {
                    $key = isset($row['fields'][$this->indexBy]) ?
                        $row['fields'][$this->indexBy] : $row['_source'][$this->indexBy];
                } else {
                    $key = \call_user_func($this->indexBy, $row);
                }
                $rows[$key] = $row;
            }
            $result['hits']['hits'] = $rows;
        }
        return $result;
    }

    public function delete($options = [])
    {
        return $this->connection->createCommand()->deleteByQuery($options);
    }

    public function scalar($field)
    {
        $record = $this->one();
        if ($record !== false) {
            if ($field === '_id') {
                return $record['_id'];
            } elseif (isset($record['_source'][$field])) {
                return $record['_source'][$field];
            } elseif (isset($record['fields'][$field])) {
                return \count($record['fields'][$field]) === 1 ?
                    \reset($record['fields'][$field]) :
                    $record['fields'][$field];
            }
        }
        return null;
    }

    public function column($field)
    {
        $command = $this->connection->createCommand();
        $command->queryParts['_source'] = [$field];
        $result = $command->search();
        if ($result === false) {
            throw new \RuntimeException('Elasticsearch search query failed.');
        }
        if (empty($result['hits']['hits'])) {
            return [];
        }
        $column = [];
        foreach ($result['hits']['hits'] as $row) {
            if (isset($row['fields'][$field])) {
                $column[] = $row['fields'][$field];
            } elseif (isset($row['_source'][$field])) {
                $column[] = $row['_source'][$field];
            } else {
                $column[] = null;
            }
        }
        return $column;
    }

    public function count($q = '*'): int
    {
        $result = $this->connection->createCommand()->search(['size' => 0]);

        if (isset($result['hits']['total'])) {
            return \is_array($result['hits']['total']) ?
                (int)$result['hits']['total']['value'] :
                (int)$result['hits']['total'];
        }
        return 0;
    }

    public function exists(): bool
    {
        return $this->one() !== false;
    }

    public function getStats()
    {
        return $this->stats;
    }

    public function stats($groups)
    {
        $this->stats = $groups;
        return $this;
    }

    public function getHighlight()
    {
        return $this->highlight;
    }

    public function highlight($highlight)
    {
        $this->highlight = $highlight;
        return $this;
    }

    public function getAggregations()
    {
        return $this->aggregations;
    }

    public function addAggregate($name, $options)
    {
        $this->aggregations[$name] = $options;
        return $this;
    }

    public function getSuggest()
    {
        return $this->suggest;
    }

    public function addSuggester($name, $definition)
    {
        $this->suggest[$name] = $definition;
        return $this;
    }

    public function getCollapse()
    {
        return $this->collapse;
    }

    public function addCollapse($collapse)
    {
        $this->collapse = $collapse;
        return $this;
    }

    public function query($query)
    {
        $this->query = $query;
        return $this;
    }

    public function batch($scrollWindow = '1m')
    {
        return new BatchQueryResult();
    }

    public function each($scrollWindow = '1m')
    {
        return new BatchQueryResult();
    }

    public function from($index, $type = null)
    {
        $this->index = $index;
        $this->type = $type;
        return $this;
    }

    public function getStoredFields()
    {
        return $this->storedFields;
    }

    public function storedFields($fields)
    {
        if (\is_array($fields) || $fields === null) {
            $this->storedFields = $fields;
        } else {
            $this->storedFields = \func_get_args();
        }
        return $this;
    }

    public function getScriptFields()
    {
        return $this->scriptFields;
    }

    public function scriptFields($fields)
    {
        if (\is_array($fields) || $fields === null) {
            $this->scriptFields = $fields;
        } else {
            $this->scriptFields = \func_get_args();
        }
        return $this;
    }

    public function getSource()
    {
        return $this->source;
    }

    public function source($source)
    {
        if (\is_array($source) || $source === null) {
            $this->source = $source;
        } else {
            $this->source = \func_get_args();
        }
        return $this;
    }

    public function getTimeout()
    {
        return $this->timeout;
    }

    public function timeout($timeout)
    {
        $this->timeout = $timeout;
        return $this;
    }

    public function getMinScore()
    {
        return $this->minScore;
    }

    public function minScore($minScore)
    {
        $this->minScore = $minScore;
        return $this;
    }

    public function getOptions()
    {
        return $this->options;
    }

    public function options($options)
    {
        if (!\is_array($options)) {
            throw new \InvalidArgumentException('Array parameter expected, ' . gettype($options) . ' received.');
        }
        $this->options = $options;
        return $this;
    }

    public function addOptions($options)
    {
        if (!\is_array($options)) {
            throw new \InvalidArgumentException('Array parameter expected, ' . gettype($options) . ' received.');
        }
        $this->options = \array_merge($this->options, $options);
        return $this;
    }

    public function andWhere($condition): self
    {
        if ($this->where === null) {
            $this->where = $condition;
        } elseif (isset($this->where[0]) && $this->where[0] === 'and') {
            $this->where[] = $condition;
        } else {
            $this->where = ['and', $this->where, $condition];
        }
        return $this;
    }

    public function orWhere($condition): self
    {
        if ($this->where === null) {
            $this->where = $condition;
        } elseif (isset($this->where[0]) && $this->where[0] === 'or') {
            $this->where[] = $condition;
        } else {
            $this->where = ['or', $this->where, $condition];
        }
        return $this;
    }

    public function getPostFilter()
    {
        return $this->postFilter;
    }

    public function postFilter($filter)
    {
        $this->postFilter = $filter;
        return $this;
    }

    public function getExplain()
    {
        return $this->explain;
    }

    public function explain($explain)
    {
        $this->explain = $explain;
        return $this;
    }

    public function getLimit()
    {
        return $this->limit;
    }

    public function getOffset()
    {
        return $this->offset;
    }

    public function getWhere()
    {
        return $this->where;
    }

    public function getQuery()
    {
        return $this->query;
    }

    public function getOrderBy()
    {
        return $this->orderBy;
    }

    public function getIndex()
    {
        return $this->index;
    }

    public function getType()
    {
        return $this->type;
    }
}
