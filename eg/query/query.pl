#!/usr/bin/perl
use strict;
use warnings;
use utf8;
use feature qw(say);

use Getopt::Long qw(:config posix_default no_ignore_case gnu_compat);

use JSON qw(encode_json);
use Google::BigQuery::Lite;

GetOptions(
    'project_id=s'       => \my $project_id,
    'dataset_id=s'       => \my $dataset_id,
    'client_email=s'     => \my $client_email,
    'private_key_file=s' => \my $private_key_file,
);

# create a instance
my $bq_lite = Google::BigQuery::Lite->new(
    client_email     => $client_email,
    private_key_file => $private_key_file,
    project_id       => $project_id,
);

# create a dataset
$bq_lite->use_dataset($dataset_id);

my $field_definitions = [
    {
        name   => 'state',
        schema => +{
            type => ['string'],
        }
    },
    {
        name   => 'gender',
        schema => +{
            type => ['string'],
        }
    },
    {
        name   => 'year',
        schema => +{
            type => ['integer'],
        }
    },
    {
        name   => 'name',
        schema => +{
            type => ['string'],
        }
    },
    {
        name   => 'number',
        schema => +{
            type => ['integer'],
        }
    },
];

# selectall_arrayref
my $rows = $bq_lite->selectall_arrayref(
    useQueryCache     => 1,
    useLegacySql      => 1,
    field_definitions => $field_definitions,
    query             => <<'SQL');
SELECT
    state,
    gender,
    year,
    name,
    number
FROM
    bigquery-public-data.usa_names.usa_1910_2013
LIMIT 10
SQL

unless ($rows) {
    die $!;
}

foreach my $row ( @{$rows} ) {
    say encode_json($row);
}
