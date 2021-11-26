<?php

declare(strict_types=1);

namespace Whirlwind\ElasticSearch\Persistence\Query;

use Whirlwind\ElasticSearch\Persistence\Connection;

class QueryBuilder
{
    protected $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function build(Query $query)
    {
        $parts = [];

        if ($query->getStoredFields() !== null) {
            $parts['stored_fields'] = $query->getStoredFields();
        }
        if ($query->getScriptFields() !== null) {
            $parts['script_fields'] = $query->getScriptFields();
        }

        if ($query->getSource() !== null) {
            $parts['_source'] = $query->getSource();
        }
        if ($query->getLimit() !== null && $query->getLimit() >= 0) {
            $parts['size'] = $query->getLimit();
        }
        if ($query->getOffset() > 0) {
            $parts['from'] = (int)$query->getOffset();
        }
        if ($query->getMinScore() !== null) {
            $parts['min_score'] = (float)$query->getMinScore();
        }
        if ($query->getExplain() !== null) {
            $parts['explain'] = $query->getExplain();
        }

        // combine query with where
        $conditionals = [];
        $whereQuery = $this->buildQueryFromWhere($query->getWhere());
        if ($whereQuery) {
            $conditionals[] = $whereQuery;
        }
        if ($query->getQuery()) {
            $conditionals[] = $query->getQuery();
        }
        if (\count($conditionals) === 2) {
            $parts['query'] = ['bool' => ['must' => $conditionals]];
        } elseif (\count($conditionals) === 1) {
            $parts['query'] = \reset($conditionals);
        }

        if (!empty($query->getHighlight())) {
            $parts['highlight'] = $query->getHighlight();
        }
        if (!empty($query->getAggregations())) {
            $parts['aggregations'] = $query->getAggregations();
        }
        if (!empty($query->getStats())) {
            $parts['stats'] = $query->getStats();
        }
        if (!empty($query->getSuggest())) {
            $parts['suggest'] = $query->getSuggest();
        }
        if (!empty($query->getPostFilter())) {
            $parts['post_filter'] = $query->getPostFilter();
        }
        if (!empty($query->getCollapse())) {
            $parts['collapse'] = $query->getCollapse();
        }

        $sort = $this->buildOrderBy($query->getOrderBy());
        if (!empty($sort)) {
            $parts['sort'] = $sort;
        }

        $options = $query->getOptions();
        if ($query->getTimeout() !== null) {
            $options['timeout'] = $query->getTimeout();
        }

        return [
            'queryParts' => $parts,
            'index' => $query->getIndex(),
            'type' => $query->getType(),
            'options' => $options,
        ];
    }

    public function buildOrderBy($columns)
    {
        if (empty($columns)) {
            return [];
        }
        $orders = [];
        foreach ($columns as $name => $direction) {
            if (\is_string($direction)) {
                $column = $direction;
                $direction = SORT_ASC;
            } else {
                $column = $name;
            }
            if ($this->connection->getDslVersion() < 7) {
                if ($column == '_id') {
                    $column = '_uid';
                }
            }

            if (\is_array($direction)) {
                $orders[] = [$column => $direction];
            } else {
                $orders[] = [$column => ($direction === SORT_DESC ? 'desc' : 'asc')];
            }
        }

        return $orders;
    }

    public function buildQueryFromWhere($condition)
    {
        $where = $this->buildCondition($condition);
        if ($where) {
            $query = [
                'constant_score' => [
                    'filter' => $where,
                ],
            ];
            return $query;
        } else {
            return null;
        }
    }

    public function buildCondition($condition)
    {
        static $builders = [
            'not' => 'buildNotCondition',
            'and' => 'buildBoolCondition',
            'or' => 'buildBoolCondition',
            'between' => 'buildBetweenCondition',
            'not between' => 'buildBetweenCondition',
            'in' => 'buildInCondition',
            'not in' => 'buildInCondition',
            'like' => 'buildLikeCondition',
            'not like' => 'buildLikeCondition',
            'or like' => 'buildLikeCondition',
            'or not like' => 'buildLikeCondition',
            'lt' => 'buildHalfBoundedRangeCondition',
            '<' => 'buildHalfBoundedRangeCondition',
            'lte' => 'buildHalfBoundedRangeCondition',
            '<=' => 'buildHalfBoundedRangeCondition',
            'gt' => 'buildHalfBoundedRangeCondition',
            '>' => 'buildHalfBoundedRangeCondition',
            'gte' => 'buildHalfBoundedRangeCondition',
            '>=' => 'buildHalfBoundedRangeCondition',
        ];

        if (empty($condition)) {
            return [];
        }
        if (!\is_array($condition)) {
            throw new \InvalidArgumentException('String conditions in where() are not supported by Elasticsearch.');
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = \strtolower($condition[0]);
            if (isset($builders[$operator])) {
                $method = $builders[$operator];
                \array_shift($condition);

                return $this->$method($operator, $condition);
            } else {
                throw new \InvalidArgumentException('Found unknown operator in query: ' . $operator);
            }
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            return $this->buildHashCondition($condition);
        }
    }

    private function buildHashCondition($condition)
    {
        $parts = $emptyFields = [];
        foreach ($condition as $attribute => $value) {
            if ($attribute == '_id') {
                if ($value === null) { // there is no null pk
                    // this condition is equal to WHERE false
                    $parts[] = ['bool' => ['must_not' => [['match_all' => new \stdClass()]]]];
                } else {
                    $parts[] = ['ids' => ['values' => \is_array($value) ? $value : [$value]]];
                }
            } else {
                if (\is_array($value)) { // IN condition
                    $parts[] = ['terms' => [$attribute => $value]];
                } else {
                    if ($value === null) {
                        $emptyFields[] = [ 'exists' => [ 'field' => $attribute ] ];
                    } else {
                        $parts[] = ['term' => [$attribute => $value]];
                    }
                }
            }
        }

        $query = [ 'must' => $parts ];
        if ($emptyFields) {
            $query['must_not'] = $emptyFields;
        }
        return [ 'bool' => $query ];
    }

    private function buildNotCondition($operator, $operands)
    {
        if (\count($operands) != 1) {
            throw new \InvalidArgumentException("Operator '$operator' requires exactly one operand.");
        }

        $operand = \reset($operands);
        if (\is_array($operand)) {
            $operand = $this->buildCondition($operand);
        }

        return [
            'bool' => [
                'must_not' => $operand,
            ],
        ];
    }

    private function buildBoolCondition($operator, $operands)
    {
        $parts = [];
        if ($operator === 'and') {
            $clause = 'must';
        } elseif ($operator === 'or') {
            $clause = 'should';
        } else {
            throw new \InvalidArgumentException("Operator should be 'or' or 'and'");
        }

        foreach ($operands as $operand) {
            if (\is_array($operand)) {
                $operand = $this->buildCondition($operand);
            }
            if (!empty($operand)) {
                $parts[] = $operand;
            }
        }
        if ($parts) {
            return [
                'bool' => [
                    $clause => $parts,
                ]
            ];
        } else {
            return null;
        }
    }

    private function buildBetweenCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new \InvalidArgumentException("Operator '$operator' requires three operands.");
        }

        [$column, $value1, $value2] = $operands;
        if ($column === '_id') {
            throw new \InvalidArgumentException('Between condition is not supported for the _id field.');
        }
        $filter = ['range' => [$column => ['gte' => $value1, 'lte' => $value2]]];
        if ($operator === 'not between') {
            $filter = ['bool' => ['must_not' => $filter]];
        }

        return $filter;
    }

    private function buildInCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1]) || !\is_array($operands)) {
            throw new \InvalidArgumentException(
                "Operator '$operator' requires array of two operands: column and values"
            );
        }

        [$column, $values] = $operands;

        $values = (array)$values;

        if (empty($values) || $column === []) {
            // this condition is equal to WHERE false
            return $operator === 'in' ? ['bool' => ['must_not' => [['match_all' => new \stdClass()]]]] : [];
        }

        if (\is_array($column)) {
            if (\count($column) > 1) {
                return $this->buildCompositeInCondition($operator, $column, $values);
            }
            $column = \reset($column);
        }
        $canBeNull = false;
        foreach ($values as $i => $value) {
            if (\is_array($value)) {
                $values[$i] = $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                $canBeNull = true;
                unset($values[$i]);
            }
        }
        if ($column === '_id') {
            if (empty($values) && $canBeNull) { // there is no null pk
                // this condition is equal to WHERE false
                $filter = ['bool' => ['must_not' => [['match_all' => new \stdClass()]]]];
            } else {
                $filter = ['ids' => ['values' => \array_values($values)]];
                if ($canBeNull) {
                    $filter = [
                        'bool' => [
                            'should' => [
                                $filter,
                                'bool' => ['must_not' => ['exists' => ['field' => $column]]],
                            ],
                        ],
                    ];
                }
            }
        } else {
            if (empty($values) && $canBeNull) {
                $filter = [
                    'bool' => [
                        'must_not' => [
                            'exists' => [ 'field' => $column ],
                        ]
                    ]
                ];
            } else {
                $filter = [ 'terms' => [$column => \array_values($values)] ];
                if ($canBeNull) {
                    $filter = [
                        'bool' => [
                            'should' => [
                                $filter,
                                'bool' => ['must_not' => ['exists' => ['field' => $column]]],
                            ],
                        ],
                    ];
                }
            }
        }

        if ($operator === 'not in') {
            $filter = [
                'bool' => [
                    'must_not' => $filter,
                ],
            ];
        }

        return $filter;
    }

    private function buildHalfBoundedRangeCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new \InvalidArgumentException("Operator '$operator' requires two operands.");
        }

        [$column, $value] = $operands;
        if ($this->connection->getDslVersion() < 7) {
            if ($column === '_id') {
                $column = '_uid';
            }
        }

        $range_operator = null;

        if (\in_array($operator, ['gte', '>='])) {
            $range_operator = 'gte';
        } elseif (\in_array($operator, ['lte', '<='])) {
            $range_operator = 'lte';
        } elseif (\in_array($operator, ['gt', '>'])) {
            $range_operator = 'gt';
        } elseif (\in_array($operator, ['lt', '<'])) {
            $range_operator = 'lt';
        }

        if ($range_operator === null) {
            throw new \InvalidArgumentException("Operator '$operator' is not implemented.");
        }

        $filter = [
            'range' => [
                $column => [
                    $range_operator => $value
                ]
            ]
        ];

        return $filter;
    }

    protected function buildCompositeInCondition($operator, $columns, $values)
    {
        throw new \InvalidArgumentException('composite in is not supported by Elasticsearch.');
    }

    private function buildLikeCondition($operator, $operands)
    {
        throw new \InvalidArgumentException('like conditions are not supported by Elasticsearch.');
    }
}
