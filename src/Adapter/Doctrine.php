<?php

declare(strict_types=1);

/*
 * This file is part of BoringSearch.
 *
 * (c) Yanick Witschi
 *
 * @license MIT
 */

namespace BoringSearch\Doctrine\Adapter;

use BoringSearch\Core\Adapter\AbstractAdapter;
use BoringSearch\Core\Adapter\RequiresSetupInterface;
use BoringSearch\Core\Index\IndexInterface;
use BoringSearch\Doctrine\Index\Index;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;

class Doctrine extends AbstractAdapter implements RequiresSetupInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function setup(): void
    {
        $table = new Table(
            'search_index',
            [
                new Column('id', Type::getType(Types::STRING)),
                new Column('index_name', Type::getType(Types::STRING)),
                new Column('content', Type::getType(Types::STRING)),
            ]
        );
        $this->connection->getSchemaManager()->createTable($table);
    }

    public function getIndex(string $name): IndexInterface
    {
        return new Index($this->connection, $this, $name);
    }
}
