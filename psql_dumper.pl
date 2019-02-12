#!/usr/bin/perl -w

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use Pod::Usage;
use DBI;
use Try::Tiny;
use SQL::Statement;

BEGIN {
    binmode(STDIN,  ":utf8");
    binmode(STDOUT, ":utf8");
    binmode(STDERR, ":utf8");
}

my $help;
my $host = "/var/run/postgresql";
my $port = 5432;
my $database;
my $username;
my $password;

my @schemas;
my @tables;
my @queries;
my $columns;
my $excluded_columns;
my $columns_regexp;
my @filters;

my $inserts;
my $updates;
my $copy;

GetOptions(
    "help|?"     => \$help,
    "database|d=s"=> \$database,
    "host|h=s"=>\$host,
    "port|p=i"=>\$port,
    "username|U=s"=>\$username,
    "password|W=s"=>\$password,
    "scheme|n=s"=>\@schemas,
    "table|t=s"=>\@tables,
    "columns|c=s"=>\$columns,
    "exclude-columns|ec=s"=>\$excluded_columns,
    "query|q=s"=>\@queries,
    "inserts"=>\$inserts,
    "updates"=>\$updates,
    "copy"=>\$copy,
    "filter|f=s"=>\@filters,
    "columns-regexp|cr=s"=>\$columns_regexp
);

my $message_text=qq(
Usage:
psql_dumper [DB CONNECTION OPTIONS]... [GENERAL DUMP OPTIONS]... [OUTPUT OPTIONS]
psql_dumper -d [DATABASE] -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -t [TABLE] -c [COLUMN],[COLUMN] || -ec [COLUMN],[COLUMN] || -cr [COLUMNS_REGEXP] -t [TABLE 2] -c [COLUMN],[COLUMN] --updates||--inserts
psql_dumper -d [DATABASE] -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -t [TABLE] -n [SCHEME]  --updates||--inserts
psql_dumper -d [DATABASE] -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -t [TABLE] -q ["SELECT_QUERY"] -q ["SELECT_QUERY"] --updates||--inserts

Minimal example:
psql_dumper -d DATABASE_NAME -t TABLE_NAME --inserts # outputs INSERT queries for all rows in given TABLE_NAME


Connection options:
-d, --database=DATABASE database name (required)
-h, --host=HOSTNAME     database server host or socket directory
-p, --port=PORT         database server port number
-U, --username=NAME     connect as specified database user
-W, --password          force password prompt (should prompt automatically)


General dump options:
-n,  --scheme           dump the named schema(s) only (if -t is specified scheme is ignored)
-t,  --table            dump the named table(s) only
-c,  --columns          dump the specific column(s) from table(s) only (works only if table is specified)
-ec, --exclude-columns  exclude columns from dump results (works only if table is specified)
-cr, --columns-regexp   dump only columns matching the regexp (works only if table is specified)
-q,  --query            dump results return by SQL SELECT statement(s) only
-f,  --filter           filter columns (primary key column/unique key columns), for updates it defaults to `id` if available as a column

Output options:
--inserts               dump only INSERT statements
--updates               dump only UPDATES statements
--copy                dump as COPY statement

!!! If no --inserts or --updates option are passed, dump results are printed in format from COPY command !!!
);

pod2usage($message_text) if $help || !defined ($database) || !defined($host);
die($message_text) if (scalar grep { defined($_) || $_ } $copy, $inserts, $updates) > 1;

# NOTE: Make copy default
$copy = 1 if (!defined($inserts) && !defined($updates));

sub GetSchemeTables
{
    my ($dbh,$scheme) = @_;

    my $select_tables_query = qq(SELECT table_name FROM information_schema.tables WHERE table_schema = ? order by table_name);
    my $sth = $dbh->prepare($select_tables_query);
    $sth->execute($scheme);
    my $tables_ary_ref = $sth->fetchall_arrayref();
    $sth->finish();

    foreach my $table (@$tables_ary_ref)
    {
        push(@tables,$table->[0]);
    }
}

sub QuoteAndJoinHash($$$)
{
    my ($dbh, $hash, $join_operator) = @_;

    if (ref $hash ne 'HASH')
    {
        die 'Second parameter must be a hash ref';
    }

    if (ref $join_operator ne '' || !$join_operator)
    {
        die 'Third parameter must be a string';
    }

    $join_operator = ' ' . $join_operator . ' ';

    my $where_str = join ($join_operator, map {my $column = $dbh->quote_identifier($_); my $value = $dbh->quote($$hash{$_}); "$column = $value" } sort keys %{ $hash });

    return $where_str;
}

sub Dump
{
    my ($dbh,$table_name,$query)=@_;


 
    my $sth=$dbh->prepare($query);
    $sth->execute();

    my @record_set_columns = @{ $sth->{NAME} };
    my $columns = join(', ', map{ $dbh->quote_identifier ($_) } @record_set_columns);
   
    # NOTE: Handles COPY
    if (defined($copy) && !defined($updates) && !defined($inserts))
    {
        print "COPY $table_name ($columns) FROM stdin;\n";

        $dbh->do("COPY ($query) TO STDOUT");

        while (1)
        {
            my $row = '';
            last if $dbh->pg_getcopydata($row) < 0;
            print $row;
        }

        print "\\.\n";

        return;
    }

    # NOTE: Handles UPDATE and INSERT
    while (my $row_ary_ref = $sth->fetchrow_arrayref())
    {
        my $values = join(', ', map{ $dbh->quote($_) }@$row_ary_ref);
        my %set = map { $record_set_columns[$_] => @$row_ary_ref[$_] } 0 .. $#record_set_columns;

        if(defined($inserts) && !defined($updates) && !defined($copy))
        {
            my $insert_statement = qq(INSERT INTO $table_name ($columns));

            if(@filters)
            {
                my $where_hash = {};

                for my $filter (@filters)
                {
                    if(!defined($set{$filter}))
                    {
                        die "There is no column '$filter' in table '$table_name' ! \n";
                    }

                    $$where_hash{$filter} = $set{$filter};
                }

                my $where_str = QuoteAndJoinHash($dbh, $where_hash, ' AND ');

                $insert_statement = qq($insert_statement SELECT $values WHERE NOT EXISTS (SELECT 1 FROM $table_name WHERE $where_str ););
            }
            else
            {
                $insert_statement = qq($insert_statement values ($values););
            }
            
            print "$insert_statement\n";
        }
        elsif(defined($updates) && !defined($inserts) && !defined($copy))
        {
            my $update_set = QuoteAndJoinHash($dbh, \%set, ', ');

            if(!@filters && defined($set{id}))
            {
                push @filters, 'id';
            }

            if(@filters)
            {
                my $where_hash = {};

                for my $filter (@filters)
                {
                    if(!defined($set{$filter}))
                    {
                        die "There is no column '$filter' in table '$table_name' ! \n";
                    }

                    $$where_hash{$filter} = $set{$filter};
                }

                my $where_str = QuoteAndJoinHash($dbh, $where_hash, ' AND ');

                print qq(UPDATE $table_name SET $update_set WHERE $where_str;\n);
            }
            else
            {
                die "Please define a filter for UPDATE statements ! (-f | --filter)\n$message_text\n";
            }
        }
        else
        {
            die($message_text);
        }
    }
    
    return;
}

sub Handler{

    my $dbh=DBI->connect("DBI:Pg:dbname = $database;host = $host;port = $port", $username, $password, {RaiseError => 1, PrintError=> 0, AutoCommit => 0});

    try
    {
        if( !@tables &&
            !@queries &&
            @schemas
        )
        {
            foreach my $scheme (@schemas)
            {
                GetSchemeTables($dbh,$scheme);
                foreach my $table_name (@tables)
                {
                    Dump($dbh,$table_name,qq(SELECT * FROM $table_name));
                }
            }
        }
        elsif( !@schemas &&
            !@queries &&
            @tables
        )
        {
            foreach my $table_name (@tables)
            {
                if( !defined ($excluded_columns) &&
                    !defined($columns_regexp) &&
                    defined($columns)
                )
                {

#When only columns are specified.

                    Dump($dbh,$table_name,qq(SELECT $columns FROM $table_name));
                }
                elsif( !defined($columns) &&
                    !defined($columns_regexp) &&
                    defined($excluded_columns)
                )
                {

#When only excluded columns are specified.

                    my @excluded_columns=split(',',$excluded_columns);
                    my $excluded_columns=join(', ', map { qq('$_') } @excluded_columns);

#Return select statement with columns that's not excluded.
                    my $sth=$dbh->prepare(qq(SELECT 'SELECT ' ||
                        ARRAY_TO_STRING(ARRAY(SELECT COLUMN_NAME::VARCHAR(50)
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME='$table_name' AND
                        COLUMN_NAME NOT IN ($excluded_columns)
                        ORDER BY ORDINAL_POSITION
                        ), ', ') || ' FROM $table_name';));
                    $sth->execute();

                    my $query=$sth->fetchrow_arrayref()->[0];

                    Dump($dbh,$table_name,$query);
                }
                elsif( !defined($columns) &&
                    !defined($excluded_columns) &&
                    defined($columns_regexp)
                )
                {

#When only columns regexp are specified.

#Return select statement with columns that match columns_regexp.
                    my $sth=$dbh->prepare(qq(SELECT 'SELECT ' ||
                        ARRAY_TO_STRING(ARRAY(SELECT COLUMN_NAME::VARCHAR(50)
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME='$table_name' AND
                        COLUMN_NAME ~* '$columns_regexp'
                        ORDER BY ORDINAL_POSITION
                        ), ', ') || ' FROM $table_name';));

                    $sth->execute();

                    my $query=$sth->fetchrow_arrayref()->[0];

                    Dump($dbh,$table_name,$query);

                }
                else
                {

#In any other way dump all columns from table

                    Dump($dbh,$table_name,qq(SELECT * FROM $table_name));
                }
            }
        }
        elsif( !@schemas &&
            !@tables &&
            @queries
        )
        {
            my $parser = SQL::Parser->new();

            foreach my $query (@queries)
            {
                my $stmt = SQL::Statement->new($query,$parser);

                if($stmt->{command} ne 'SELECT')
                {
                    die "Invalid SELECT statement !\n";
                }

                my @tables_names;
                map { push(@tables_names,$_->{name})} $stmt->tables();
                my $table_name=$tables_names[0];

                Dump($dbh,$table_name,$query);
            }

        }
        else
        {
            die "Invalid parameters ! Please specified only one of the options -> schema, table(s) or query(queries) !\n$message_text\n";
        }


        $dbh->disconnect();
    }
    catch
    {
        warn "Error: $_";
        $dbh->disconnect();
    };
}



Handler();
