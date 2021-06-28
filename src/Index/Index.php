<?php

declare(strict_types=1);

/*
 * This file is part of BoringSearch.
 *
 * (c) Yanick Witschi
 *
 * @license MIT
 */

namespace BoringSearch\Doctrine\Index;

use BoringSearch\Core\Document\Attribute\Attribute;
use BoringSearch\Core\Document\Attribute\AttributeCollection;
use BoringSearch\Core\Document\Document;
use BoringSearch\Core\Document\DocumentInterface;
use BoringSearch\Core\Index\AbstractIndex;
use BoringSearch\Core\Index\Result\ResultInterface;
use BoringSearch\Core\Index\Result\SynchronousResult;
use BoringSearch\Core\Query\QueryInterface;
use BoringSearch\Core\Query\Result\QueryResult;
use BoringSearch\Core\Query\Result\QueryResultInterface;
use BoringSearch\Core\Query\Result\Result;
use BoringSearch\Core\Query\Result\ResultCollection;
use BoringSearch\Doctrine\Adapter\Doctrine;
use Doctrine\DBAL\Connection;

class Index extends AbstractIndex
{
    private Connection $connection;

    public function __construct(Connection $connection, Doctrine $adapter, string $name)
    {
        $this->connection = $connection;

        parent::__construct($adapter, $name);
    }

    public function query(QueryInterface $query): QueryResultInterface
    {
        $db = $this->connection->createQueryBuilder()
            ->select('*')
            ->from('search_index')
            ->where('index_name=:index')
            ->andWhere('content LIKE :keywords')
            ->setParameters([
                'index' => $this->getName(),
                'keywords' => '%'.$query->getSearchString().'%',
            ])
            ->setFirstResult($query->getOffset())
            ->setMaxResults($query->getLimit())
            ->executeQuery()
        ;

        $results = [];

        foreach ($db->fetchAllAssociative() as $row) {
            $results[] = new Result($this->createDocumentFromRow($row, $query->getAttributeNamesToRetrieve()));
        }

        return new QueryResult($query, new ResultCollection($results), \count($results), false);
    }

    public function findByIdentifier(string $identifier): ?DocumentInterface
    {
        $db = $this->connection->fetchAssociative('SELECT * FROM search_index WHERE index_name=:index AND id=:id', [
            'index' => $this->getName(),
            'id' => $identifier,
        ]);

        if (false === $db) {
            return null;
        }

        return $this->createDocumentFromRow($db);
    }

    public function doDelete(array $identifiers): ResultInterface
    {
        $stmt = $this->connection->prepare('DELETE FROM search_index WHERE index_name=:index AND id IN (:ids)');
        $stmt->executeQuery([
            'index' => $this->getName(),
            'ids' => implode(',', $identifiers),
        ]);

        return new SynchronousResult(true);
    }

    public function doPurge(): ResultInterface
    {
        $this->connection->delete('search_index', ['index_name' => $this->getName()]);

        return new SynchronousResult(true);
    }

    /**
     * @param array<DocumentInterface> $documents
     */
    protected function doIndex(array $documents): ResultInterface
    {
        // TODO: Could do batch inserts here
        foreach ($documents as $document) {
            $this->connection->insert(
                'search_index',
                [
                    'id' => $document->getIdentifier(),
                    'index_name' => $this->getName(),
                    'content' => json_encode($document->getAttributes()),
                ]
            );
        }

        return new SynchronousResult(true);
    }

    private function createDocumentFromRow(array $row, array $attributeNamesToRetrieve = []): DocumentInterface
    {
        $attributes = new AttributeCollection();
        $data = json_decode($row['content'], true);

        foreach ($data as $k => $v) {
            if ([] !== $attributeNamesToRetrieve && !\in_array($k, $attributeNamesToRetrieve, true)) {
                continue;
            }

            $attributes->addAttribute(new Attribute($k, $v));
        }

        return new Document($row['id'], $attributes);
    }
}
