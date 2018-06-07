use strict;
use warnings;
package Semja;

# ABSTRACT: database seeding script

use DBI;
use strict;
use warnings;
use v5.20.2;
use Data::Dump qw/dump/;

# Wackly, we have to use file::slurp instead of slurper because of jessie.
use File::Slurp qw/read_file/;
use JSON;
use autodie qw(:all);
use YAML qw(Load);
use Text::Template;

my $dbh;

sub initialize {
    my $conf_text = read_file "/usr/local/etc/semja.json";
    my $conf = decode_json $conf_text;
    say dump($conf);

    my $driver   = "Pg"; 
    my $database = $conf->{database};
    my $host = $conf->{hostname};
    my $port = $conf->{port};


    my $dsn = "DBI:${driver}:dbname=${database};host=${host};port=${port}";

    my $userid = $conf->{username};
    my $password = $conf->{password};

    $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
        or die $DBI::errstr;
    print "Opened database successfully\n";
}

# The script will create a temp table based on the structure
# using CREATE TEMPORARY TABLE foo AS SELECT * FROM orig_table
# Then the rows will be bulk inserted into the temporary table.
# Rows defines the rows to copy.
# There MUST always be a surrogate key id to join on, otherwise stuff will 
# break.  The operation of an upsert doesn't make sense without having a key.

my $setup_ddl_template = '
BEGIN;

CREATE TEMPORARY TABLE source_data
ON COMMIT DROP
AS SELECT * FROM {$original_table}
WITH NO DATA;
';

# Somehow do the actual insert
my $insert_template = q{
INSERT INTO source_data({$key}{$column_insert_expression})
VALUES ({$placeholder_expression})
};

# Create the list of columns.

my $upsert_query_template_part1 = '
LOCK TABLE {$original_table} IN EXCLUSIVE MODE;
';

# This part is conditional, that's why we had to split it into three parts.
my $upsert_query_template_part2 = '
UPDATE {$original_table} t1
SET {$update_set_expression}
FROM source_data
WHERE t1.{$key} = source_data.{$key};
';

my $upsert_query_template_part3 = '
INSERT INTO {$original_table}
SELECT source_data.*
FROM source_data
LEFT OUTER JOIN {$original_table} ON ({$original_table}.{$key} = source_data.{$key})
WHERE {$original_table}.{$key} IS NULL;

COMMIT;
';

sub expand_upsert_template {
    my ($template, $original_table, $columns, $key) = @_;

    my $tmpl_obj = Text::Template->new(
        TYPE => 'STRING',
        SOURCE => $template
        ) or die "Something bad happened";

    my %vars = (
        original_table => $original_table,
        update_set_expression => form_update_set_expression(@$columns),
        key => $key
    );

    my $result = $tmpl_obj->fill_in(HASH => \%vars);

    return $result;
}

sub get_upsert_query {
    my ($original_table, $columns, $key) = @_;

    my $full_query = "";

    # Partial application to parameterize
    my $expander = sub {
        my $template = shift;
        return expand_upsert_template($template, $original_table, $columns, $key);
    };

    $full_query .= $expander->($upsert_query_template_part1);

    if (@$columns) {
        $full_query .= $expander->($upsert_query_template_part2);
    }


    $full_query .= $expander->($upsert_query_template_part3);

    return $full_query;
}


sub form_update_set_expression {
    return join(", ", map { "$_ = source_data.$_" } @_);
}

sub form_column_insert_expression {
    if (@_) {
        return ", " . join(", ",  @_);
    } else {
        return "";
    }
}

sub form_placeholder_expression {
    my ($n_values) = @_;

    return join(",", map { "?" } 1..$n_values);
}

sub get_insert_query {
    my ($columns, $key) = @_;

    my $tmpl_obj = Text::Template->new(
        TYPE => 'STRING',
        SOURCE => $insert_template
        ) or die "no";
    
    my %vars = (
        key => $key,
        placeholder_expression => form_placeholder_expression(scalar(@$columns) + 1),
        column_insert_expression => form_column_insert_expression(@$columns)
    );
    my $result = $tmpl_obj->fill_in(HASH => \%vars);

    return $result;

}

sub insert_to_source {
    my ($query, $row) = @_;

    say "Inserting to the source.";
    say dump($row);
    say $query;

    $dbh->do($query, undef, @$row);
}

sub get_setup_ddl {
    my ($table) = @_;

    say "Getting the setup DDL for $table";

    my $tmpl_obj = Text::Template->new(
        TYPE => 'STRING',
        SOURCE => $setup_ddl_template
        ) or die "no";
    
    my %vars = (
        original_table => $table
    );
    my $result = $tmpl_obj->fill_in(HASH => \%vars);

    return $result;
}

sub run_upsert {
    my ($upsert_query) = @_;
    
    say $upsert_query;
    say "Running the upsert.";
    $dbh->do($upsert_query);
}

sub run_task {
    my $task = shift;
    say dump($task);

    my $setup_ddl = get_setup_ddl($task->{table});
    say "Setup DDL is " . dump($setup_ddl);
    $dbh->do($setup_ddl) or die "foo";


    my $insert_query = get_insert_query($task->{columns}, $task->{key});
    say "Using insert query: $insert_query";

    for my $row (@{$task->{rows}}) {
        insert_to_source($insert_query, $row);
    }
    
    my $concrete_upsert = 
      get_upsert_query($task->{table}, $task->{columns}, $task->{key});

    run_upsert($concrete_upsert);
}

sub main {
    my @merged_tasks;

    for my $file (@ARGV) {
        my $file_text = read_file($file);
        my $val = Load($file_text);

        if ($val) {
            say dump($val);

            for my $task (@{$val->{tasks}}) {
                push @merged_tasks, $task;
            }
        } else {
            die "Some loaded file contained nothing.";
        }
    }


    say dump(\@merged_tasks);
    say "Will run " . scalar(@merged_tasks) . " tasks.";


    for my $real_task (@merged_tasks) {
        run_task($real_task);
    }
}



1;
