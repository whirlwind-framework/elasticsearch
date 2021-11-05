<?php declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence;

use Whirlwind\Infrastructure\Persistence\ConnectionInterface;

class Connection implements ConnectionInterface
{
    protected $autodetectCluster = true;

    protected $nodes;

    protected $activeNode;

    protected $auth;

    protected $defaultProtocol;

    protected $connectionTimeout;

    protected $dataTimeout;

    protected $dslVersion;

    protected $curl;

    public function __construct(
        array $nodes = [['http_address' => 'inet[/127.0.0.1:9200]']],
        bool $autodetectCluster = true,
        string $activeNode = '',
        array $auth = [],
        string $defaultProtocol = 'http',
        int $dslVersion = 5,
        $connectionTimeout = null,
        $dataTimeout = null
    ) {
        $this->nodes = $nodes;
        $this->autodetectCluster = $autodetectCluster;
        $this->activeNode = $activeNode;
        $this->auth = $auth;
        $this->defaultProtocol = $defaultProtocol;
        $this->dslVersion = $dslVersion;
        $this->connectionTimeout = $connectionTimeout;
        $this->dataTimeout = $dataTimeout;
        foreach ($this->nodes as &$node) {
            if (!isset($node['http_address'])) {
                throw new \InvalidArgumentException('Elasticsearch node needs at least a http_address configured.');
            }
            if (!isset($node['protocol'])) {
                $node['protocol'] = $this->defaultProtocol;
            }
            if (!\in_array($node['protocol'], ['http', 'https'])) {
                throw new \InvalidArgumentException('Valid node protocol settings are "http" and "https".');
            }
        }
    }

    public function __sleep()
    {
        $this->close();
        return \array_keys(\get_object_vars($this));
    }

    public function getIsActive()
    {
        return $this->activeNode !== null;
    }

    public function open()
    {
        if ($this->activeNode !== null) {
            return;
        }
        if (empty($this->nodes)) {
            throw new \InvalidArgumentException('Elasticsearch needs at least one node to operate.');
        }
        $this->curl = \curl_init();
        if ($this->autodetectCluster) {
            $this->populateNodes();
        }
        $this->selectActiveNode();
    }

    protected function populateNodes()
    {
        $node = \reset($this->nodes);
        $host = $node['http_address'];
        $protocol = isset($node['protocol']) ? $node['protocol'] : $this->defaultProtocol;
        if (\strncmp($host, 'inet[/', 6) === 0) {
            $host = \substr($host, 6, -1);
        }
        $response = $this->httpRequest('GET', "$protocol://$host/_nodes/_all/http");
        if (!empty($response['nodes'])) {
            $nodes = $response['nodes'];
        } else {
            $nodes = [];
        }

        foreach ($nodes as $key => &$node) {
            if (!isset($node['http']['publish_address'])) {
                unset($nodes[$key]);
            }
            $node['http_address'] = $node['http']['publish_address'];

            $node['protocol'] = $this->defaultProtocol;
        }

        if (!empty($nodes)) {
            $this->nodes = \array_values($nodes);
        } else {
            \curl_close($this->curl);
            throw new \RuntimeException('Cluster autodetection did not find any active node. Make sure a GET /_nodes reguest on the hosts defined in the config returns the "http_address" field for each node.');
        }
    }

    protected function selectActiveNode()
    {
        $keys = \array_keys($this->nodes);
        $this->activeNode = $keys[\rand(0, \count($keys) - 1)];
    }

    public function close()
    {
        if ($this->activeNode === null) {
            return;
        }
        $this->activeNode = null;
        if ($this->curl) {
            \curl_close($this->curl);
            $this->curl = null;
        }
    }

    public function createCommand(array $options = []): Command
    {
        $this->open();
        $command = new Command($this, $options);

        return $command;
    }

    public function createBulkCommand($config = []): BulkCommand
    {
        $this->open();
        $command = new BulkCommand($this);

        return $command;
    }

    public function getQueryBuilder(): QueryBuilder
    {
        return new QueryBuilder($this);
    }

    public function get($url, $options = [], $body = null, $raw = false)
    {
        $this->open();
        return $this->httpRequest('GET', $this->createUrl($url, $options), $body, $raw);
    }

    public function head($url, $options = [], $body = null)
    {
        $this->open();
        return $this->httpRequest('HEAD', $this->createUrl($url, $options), $body);
    }

    public function post($url, $options = [], $body = null, $raw = false)
    {
        $this->open();
        return $this->httpRequest('POST', $this->createUrl($url, $options), $body, $raw);
    }

    public function put($url, $options = [], $body = null, $raw = false)
    {
        $this->open();
        return $this->httpRequest('PUT', $this->createUrl($url, $options), $body, $raw);
    }

    public function delete($url, $options = [], $body = null, $raw = false)
    {
        $this->open();
        return $this->httpRequest('DELETE', $this->createUrl($url, $options), $body, $raw);
    }

    private function createUrl($path, $options = [])
    {
        if (!\is_string($path)) {
            $url = \implode('/', \array_map(function ($a) {
                return \urlencode(\is_array($a) ? \implode(',', $a) : $a);
            }, $path));
            if (!empty($options)) {
                $url .= '?' . \http_build_query($options);
            }
        } else {
            $url = $path;
            if (!empty($options)) {
                $url .= (\strpos($url, '?') === false ? '?' : '&') . \http_build_query($options);
            }
        }

        $node = $this->nodes[$this->activeNode];
        $protocol = isset($node['protocol']) ? $node['protocol'] : $this->defaultProtocol;
        $host = $node['http_address'];

        return [$protocol, $host, $url];
    }

    protected function httpRequest($method, $url, $requestBody = null, $raw = false)
    {
        $method = strtoupper($method);

        $headers = [];
        $headersFinished = false;
        $body = '';

        $options = [
            CURLOPT_RETURNTRANSFER => false,
            CURLOPT_HEADER         => false,
            CURLOPT_HTTPHEADER     => [
                'Expect:',
                'Content-Type: application/json',
            ],
            CURLOPT_WRITEFUNCTION  => function ($curl, $data) use (&$body) {
                $body .= $data;
                return \mb_strlen($data, '8bit');
            },
            CURLOPT_HEADERFUNCTION => function ($curl, $data) use (&$headers, &$headersFinished) {
                if ($data === '') {
                    $headersFinished = true;
                } elseif ($headersFinished) {
                    $headersFinished = false;
                }
                if (!$headersFinished && ($pos = \strpos($data, ':')) !== false) {
                    $headers[\strtolower(\substr($data, 0, $pos))] = \trim(\substr($data, $pos + 1));
                }
                return \mb_strlen($data, '8bit');
            },
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_FORBID_REUSE   => false,
        ];

        if (!empty($this->auth) || isset($this->nodes[$this->activeNode]['auth']) && $this->nodes[$this->activeNode]['auth'] !== false) {
            $auth = isset($this->nodes[$this->activeNode]['auth']) ? $this->nodes[$this->activeNode]['auth'] : $this->auth;
            if (empty($auth['username'])) {
                throw new \InvalidArgumentException('Username is required to use authentication');
            }
            if (empty($auth['password'])) {
                throw new \InvalidArgumentException('Password is required to use authentication');
            }

            $options[CURLOPT_HTTPAUTH] = CURLAUTH_BASIC;
            $options[CURLOPT_USERPWD] = $auth['username'] . ':' . $auth['password'];
        }

        if ($this->connectionTimeout !== null) {
            $options[CURLOPT_CONNECTTIMEOUT] = $this->connectionTimeout;
        }
        if ($this->dataTimeout !== null) {
            $options[CURLOPT_TIMEOUT] = $this->dataTimeout;
        }
        if ($requestBody !== null) {
            $options[CURLOPT_POSTFIELDS] = $requestBody;
        }
        if ($method == 'HEAD') {
            $options[CURLOPT_NOBODY] = true;
            unset($options[CURLOPT_WRITEFUNCTION]);
        } else {
            $options[CURLOPT_NOBODY] = false;
        }

        list($protocol, $host, $q) = $url;
        if (\strncmp($host, 'inet[', 5) == 0) {
            $host = \substr($host, 5, -1);
            if (($pos = \strpos($host, '/')) !== false) {
                $host = \substr($host, $pos + 1);
            }
        }
        $url = "$protocol://$host/$q";

        $this->resetCurlHandle();
        \curl_setopt($this->curl, CURLOPT_URL, $url);
        \curl_setopt_array($this->curl, $options);
        if (\curl_exec($this->curl) === false) {
            throw new \RuntimeException('Elasticsearch request failed: ' . \curl_errno($this->curl) . ' - ' . \curl_error($this->curl));
        }

        $responseCode = \curl_getinfo($this->curl, CURLINFO_HTTP_CODE);

        if ($responseCode >= 200 && $responseCode < 300) {
            if ($method === 'HEAD') {
                return true;
            } else {
                if (isset($headers['content-length']) && ($len = \mb_strlen($body, '8bit')) < $headers['content-length']) {
                    throw new \RuntimeException("Incomplete data received from Elasticsearch: $len < {$headers['content-length']}");
                }
                if (isset($headers['content-type'])) {
                    if (!\strncmp($headers['content-type'], 'application/json', 16)) {
                        return $raw ? $body : \json_decode($body, true);
                    }
                    if (!\strncmp($headers['content-type'], 'text/plain', 10)) {
                        return $raw ? $body : \array_filter(\explode("\n", $body));
                    }
                }
                throw new \RuntimeException('Unsupported data received from Elasticsearch: ' . $headers['content-type']);
            }
        } elseif ($responseCode == 404) {
            return false;
        } else {
            throw new \RuntimeException("Elasticsearch request failed with code $responseCode. Response body:\n{$body}");
        }
    }

    private function resetCurlHandle()
    {
        static $unsetValues = [
            CURLOPT_HEADERFUNCTION => null,
            CURLOPT_WRITEFUNCTION => null,
            CURLOPT_READFUNCTION => null,
            CURLOPT_PROGRESSFUNCTION => null,
            CURLOPT_POSTFIELDS => null,
        ];
        \curl_setopt_array($this->curl, $unsetValues);
        if (\function_exists('curl_reset')) {
            \curl_reset($this->curl);
        }
    }

    public function getNodeInfo()
    {
        return $this->get([]);
    }

    public function getClusterState()
    {
        return $this->get(['_cluster', 'state']);
    }

    public function getDslVersion(): int
    {
        return $this->dslVersion;
    }
}
