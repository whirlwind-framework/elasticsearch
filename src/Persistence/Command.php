<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence;

class Command
{
    protected Connection $connection;

    protected ?string $index = null;

    protected ?string $type = null;

    protected array $queryParts = [];

    protected array $options;

    public function __construct(
        Connection $connection,
        string $index = '',
        ?string $type = null,
        array $queryParts = [],
        array $options = []
    ) {
        $this->connection = $connection;
        $this->index = $index;
        $this->type = $type;
        $this->queryParts = $queryParts;
        $this->options = $options;
    }

    public function search($options = [])
    {
        $query = $this->queryParts;
        if (empty($query)) {
            $query = '{}';
        }
        if (\is_array($query)) {
            $query = \json_encode($query);
        }
        $url = [$this->index !== null ? $this->index : '_all'];

        if ($this->connection->getDslVersion() < 7 && $this->type !== null) {
            $url[] = $this->type;
        }

        $url[] = '_search';

        return $this->connection->get($url, \array_merge($this->options, $options), $query);
    }

    public function deleteByQuery($options = [])
    {
        if (!isset($this->queryParts['query'])) {
            throw new \RuntimeException('Can not call deleteByQuery when no query is given.');
        }
        $query = [
            'query' => $this->queryParts['query'],
        ];
        if (isset($this->queryParts['filter'])) {
            $query['filter'] = $this->queryParts['filter'];
        }
        $query = \json_encode($query);
        $url = [$this->index !== null ? $this->index : '_all'];
        if ($this->type !== null) {
            $url[] = $this->type;
        }
        $url[] = '_delete_by_query';

        return $this->connection->post($url, \array_merge($this->options, $options), $query);
    }

    public function suggest($suggester, $options = [])
    {
        if (empty($suggester)) {
            $suggester = '{}';
        }
        if (\is_array($suggester)) {
            $suggester = \json_encode($suggester);
        }
        $body = '{"suggest":' . $suggester . ',"size":0}';
        $url = [
            $this->index !== null ? $this->index : '_all',
            '_search'
        ];

        $result = $this->connection->post($url, \array_merge($this->options, $options), $body);

        return $result['suggest'];
    }

    public function insert($index, $type, $data, $id = null, $options = [])
    {
        if (empty($data)) {
            $body = '{}';
        } else {
            $body = \is_array($data) ? \json_encode($data) : $data;
        }

        if ($id !== null) {
            if ($this->connection->getDslVersion() >= 7) {
                return $this->connection->put([$index, '_doc', $id], $options, $body);
            } else {
                return $this->connection->put([$index, $type, $id], $options, $body);
            }
        } else {
            if ($this->connection->getDslVersion() >= 7) {
                return $this->connection->post([$index, '_doc'], $options, $body);
            } else {
                return $this->connection->post([$index, $type], $options, $body);
            }
        }
    }

    public function get($index, $type, $id, $options = [])
    {
        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->get([$index, '_doc', $id], $options);
        } else {
            return $this->connection->get([$index, $type, $id], $options);
        }
    }

    public function mget($index, $type, $ids, $options = [])
    {
        $body = \json_encode(['ids' => \array_values($ids)]);

        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->get([$index, '_doc', '_mget'], $options, $body);
        } else {
            return $this->connection->get([$index, $type, '_mget'], $options, $body);
        }
    }

    public function getSource($index, $type, $id)
    {
        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->get([$index, '_doc', $id]);
        } else {
            return $this->connection->get([$index, $type, $id]);
        }
    }

    public function exists($index, $type, $id)
    {
        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->head([$index, '_doc', $id]);
        } else {
            return $this->connection->head([$index, $type, $id]);
        }
    }

    public function delete($index, $type, $id, $options = [])
    {
        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->delete([$index, '_doc', $id], $options);
        } else {
            return $this->connection->delete([$index, $type, $id], $options);
        }
    }

    public function update($index, $type, $id, $data, $options = [])
    {
        $body = [
            'doc' => empty($data) ? new \stdClass() : $data,
        ];
        if (isset($options["detect_noop"])) {
            $body["detect_noop"] = $options["detect_noop"];
            unset($options["detect_noop"]);
        }

        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->post([$index, '_doc', $id, '_update'], $options, \json_encode($body));
        } else {
            return $this->connection->post([$index, $type, $id, '_update'], $options, \json_encode($body));
        }
    }

    public function createIndex($index, $configuration = null)
    {
        $body = $configuration !== null ? \json_encode($configuration) : null;

        return $this->connection->put([$index], [], $body);
    }

    public function deleteIndex($index)
    {
        return $this->connection->delete([$index]);
    }

    public function deleteAllIndexes()
    {
        return $this->connection->delete(['_all']);
    }

    public function indexExists($index)
    {
        return $this->connection->head([$index]);
    }

    public function typeExists($index, $type)
    {
        if ($this->connection->getDslVersion() >= 7) {
            return $this->connection->head([$index, '_doc']);
        } else {
            return $this->connection->head([$index, $type]);
        }
    }

    public function aliasExists($alias)
    {
        $indexes = $this->getIndexesByAlias($alias);
        return !empty($indexes);
    }

    public function getAliasInfo()
    {
        $aliasInfo = $this->connection->get(['_alias', '*']);
        return $aliasInfo ?: [];
    }

    public function getIndexInfoByAlias($alias)
    {
        $responseData = $this->connection->get(['_alias', $alias]);
        if (empty($responseData)) {
            return [];
        }
        return $responseData;
    }

    public function getIndexesByAlias($alias)
    {
        return \array_keys($this->getIndexInfoByAlias($alias));
    }

    public function getIndexAliases($index)
    {
        $responseData = $this->connection->get([$index, '_alias', '*']);
        if (empty($responseData)) {
            return [];
        }
        return $responseData[$index]['aliases'];
    }

    public function addAlias($index, $alias, $aliasParameters = [])
    {
        return (bool)$this->connection->put([$index, '_alias', $alias], [], \json_encode((object)$aliasParameters));
    }

    public function removeAlias($index, $alias)
    {
        return (bool)$this->connection->delete([$index, '_alias', $alias]);
    }

    public function aliasActions(array $actions)
    {
        return (bool)$this->connection->post(['_aliases'], [], \json_encode(['actions' => $actions]));
    }

    public function updateSettings($index, $setting, $options = [])
    {
        $body = $setting !== null ? (\is_string($setting) ? $setting : \json_encode($setting)) : null;
        return $this->connection->put([$index, '_settings'], $options, $body);
    }

    public function updateAnalyzers($index, $setting, $options = [])
    {
        $this->closeIndex($index);
        $result = $this->updateSettings($index, $setting, $options);
        $this->openIndex($index);
        return $result;
    }

    public function openIndex($index)
    {
        return $this->connection->post([$index, '_open']);
    }

    public function closeIndex($index)
    {
        return $this->connection->post([$index, '_close']);
    }

    public function scroll($options = [])
    {
        $body = \array_filter([
            'scroll' => $this->removeFromArray($options, 'scroll', null),
            'scroll_id' => $this->removeFromArray($options, 'scroll_id', null),
        ]);
        if (empty($body)) {
            $body = (object) [];
        }
        return $this->connection->post(['_search', 'scroll'], $options, \json_encode($body));
    }

    protected function removeFromArray(&$array, $key, $default = null)
    {
        if (\is_array($array) && (isset($array[$key]) || \array_key_exists($key, $array))) {
            $value = $array[$key];
            unset($array[$key]);
            return $value;
        }
        return $default;
    }

    public function clearScroll($options = [])
    {
        $body = \array_filter([
            'scroll_id' => $this->removeFromArray($options, 'scroll_id', null),
        ]);
        if (empty($body)) {
            $body = (object) [];
        }
        return $this->connection->delete(['_search', 'scroll'], $options, \json_encode($body));
    }

    public function getIndexStats($index = '_all')
    {
        return $this->connection->get([$index, '_stats']);
    }

    public function getIndexRecoveryStats($index = '_all')
    {
        return $this->connection->get([$index, '_recovery']);
    }

    public function clearIndexCache($index)
    {
        return $this->connection->post([$index, '_cache', 'clear']);
    }

    public function flushIndex($index = '_all')
    {
        return $this->connection->post([$index, '_flush']);
    }

    public function refreshIndex($index)
    {
        return $this->connection->post([$index, '_refresh']);
    }

    public function setMapping($index, $type, $mapping, $options = [])
    {
        $body = $mapping !== null ? (\is_string($mapping) ? $mapping : \json_encode($mapping)) : null;
        if ($this->connection->getDslVersion() >= 7) {
            $endpoint = [$index, '_mapping'];
        } else {
            $endpoint = [$index, '_mapping', $type];
        }
        return $this->connection->put($endpoint, $options, $body);
    }

    public function getMapping($index = '_all', $type = null)
    {
        $url = [$index, '_mapping'];
        if ($this->connection->getDslVersion() < 7 && $type !== null) {
            $url[] = $type;
        }
        return $this->connection->get($url);
    }

    public function createTemplate($name, $pattern, $settings, $mappings, $order = 0)
    {
        $body = \json_encode([
            'template' => $pattern,
            'order' => $order,
            'settings' => (object) $settings,
            'mappings' => (object) $mappings,
        ]);
        return $this->connection->put(['_template', $name], [], $body);
    }

    public function deleteTemplate($name)
    {
        return $this->connection->delete(['_template', $name]);
    }

    public function getTemplate($name)
    {
        return $this->connection->get(['_template', $name]);
    }
}
