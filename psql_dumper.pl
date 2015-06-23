#!/usr/bin/perl -w

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use Pod::Usage;
use DBI;
use Try::Tiny;
use Term::ReadKey;
use SQL::Statement;

my ($help,$database,$host,$port,$username,$password,@schemes,@tables,$columns,$excluded_columns,@queries,$inserts,$updates,$filter,$columns_regexp)=@_;

GetOptions(
    "help|?"     => \$help,
    "database|d=s"=> \$database,
    "host|h=s"=>\$host,
    "port|p=i"=>\$port,
    "username|U=s"=>\$username,
    "password|W=s"=>\$password,
    "scheme|n=s"=>\@schemes,
    "table|t=s"=>\@tables,
    "columns|c=s"=>\$columns,
    "exclude-columns|ec=s"=>\$excluded_columns,
    "query|q=s"=>\@queries,
    "inserts"=>\$inserts,
    "updates"=>\$updates,
    "filter|f=s"=>\$filter,
    "columns-regexp|cr=s"=>\$columns_regexp
);

my $message_text=qq(
Usage:
psql_dumper [DB CONNECTION OPTIONS]... [GENERAL DUMP OPTIONS]... [OUTPUT OPTIONS]
psql_dumper -d [DATABASE] -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -t [TABLE] -c [COLUMN],[COLUMN] || -ec [COLUMN],[COLUMN] || -cr [COLUMNS_REGEXP] -t [TABLE 2] -c [COLUMN],[COLUMN] --updates||--inserts
psql_dumper -d [DATABASE] -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -t [TABLE] -n [SCHEME]  --updates||--inserts
psql_dumper -d [DATABASE] -h [HOSTNAME] -p [PORT] -U [USERNAME] -W -t [TABLE] -q ["SELECT_QUERY"] -q ["SELECT_QUERY"] --updates||--inserts



Connection options:
-d, --database=DATABASE  database name (required)
-h, --host=HOSTNAME      database server host or socket directory (required)
-p, --port=PORT          database server port number (required)
-U, --username=NAME      connect as specified database user (required)
-W, --password           force password prompt (should happen automatically)


General dump options:
-n, --scheme             dump the named schema(s) only (if -t is specified scheme is ignored)
-t, --table              dump the named table(s) only
-c, --columns            dump the specific column(s) from table(s) only (works only if table is specified)
-ec, --exclude-columns   exclude columns from dump results (works only if table is specified)
-cr, --columns_regexp    dump only columns matching the regexp (works only if table is specified)
-q, --query              dump results return by SQL SELECT statement(s) only

Output options:
--inserts                dump only INSERT statements
--updates                dump only UPDATES statements

!!! If no --inserts or --updates option are passed, dump results are printed in format from COPY command !!!
);

pod2usage($message_text) if $help || !defined ($database) || !defined($host);

sub EnterPassword
{
    print STDERR "Enter password: ";
    ReadMode 'noecho';
    $password = ReadLine 0;
    chomp $password;
    ReadMode 'normal';
    print "\n";
}

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


sub Dump
{
    my ($dbh,$table_name,$query)=@_;

    my $sth=$dbh->prepare($query);
    $sth->execute();

    my @record_set_columns = @{ $sth->{NAME} };
    my $columns = join(', ', map{ $dbh->quote_identifier ($_) } @record_set_columns);

    while (my $row_ary_ref = $sth->fetchrow_arrayref())
    {

        my $values = join(', ', map{ $dbh->quote($_) }@$row_ary_ref);

        if(!defined($updates) && defined($inserts))
        {
            my $insert_statement = qq(INSERT INTO $table_name ($columns) values ($values););
            print "$insert_statement\n";
        }
        elsif(!defined($inserts) && defined($updates))
        {
            my %set = map { $record_set_columns[$_] => @$row_ary_ref[$_] } 0 .. $#record_set_columns;
            my $update_set=join (', ', map {my $column = $dbh->quote_identifier($_); my $value = $dbh->quote($set{$_}); "$column = $value" } keys %set);

            if(!defined($filter) && defined($set{id}))
            {
                my $update_statement=qq(UPDATE $table_name SET $update_set WHERE id = $set{id};);
                print "$update_statement\n";
            }
            elsif(defined ($filter))
            {
                if(defined($set{$filter}))
                {
                    my $update_statement=qq(UPDATE $table_name SET $update_set WHERE $filter = $set{$filter};);
                    print "$update_statement\n";
                }
                else
                {
                    die "There is no column '$filter' in table '$table_name' ! \n";
                }
            }
            else
            {
                die "Please define a filter for UPDATE statements ! (-f | --filter)\n$message_text\n";
            }
        }
        else
        {
            my $row;
            foreach my $col (@$row_ary_ref)
            {
                if(defined($col))
                {
                    $row.="$col\t";
                }
                else
                {
                    $row.="\\N\t";
                }
            }
            print "$row\n";
        }
    }

    if(!defined($inserts) && !defined($updates))
    {
        print "\\.\n";
    }


}

sub Handler{

    if( defined($username) && !defined( $password ) )
    {
        EnterPassword();
    }

    $port = defined( $port ) ? $port : "5432";

    my $dbh=DBI->connect("DBI:Pg:dbname = $database;host = $host;port = $port", $username, $password, {RaiseError => 1, PrintError=> 0, AutoCommit => 0});

    try
    {
        if(!@tables && !@queries && @schemes)
        {
            foreach my $scheme (@schemes)
            {
                GetSchemeTables($dbh,$scheme);
                foreach my $table_name (@tables)
                {
                    Dump($dbh,$table_name,qq(SELECT * FROM $table_name));
                }
            }
        }
        elsif(!@schemes && !@queries && @tables)
        {
            foreach my $table_name (@tables)
            {
                if(!defined ($excluded_columns) && !defined($columns_regexp) && defined($columns))
                {

#When only columns are specified.

                    Dump($dbh,$table_name,qq(SELECT $columns FROM $table_name));
                }
                elsif(!defined($columns) && !defined($columns_regexp) && defined($excluded_columns))
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
                elsif(!defined($columns) && !defined($excluded_columns) && defined($columns_regexp))
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
        elsif(!@schemes && !@tables && @queries)
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
            die "Invalid parameters ! Please specified just one of the options -> sheme, table(s) or query(queries) !\n$message_text\n";
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
