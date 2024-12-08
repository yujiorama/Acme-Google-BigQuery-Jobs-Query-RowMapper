
# NAME

Google::BigQuery::Lite - It's new $module

# SYNOPSIS

    use Google::BigQuery::Lite;

    # create a instance
    my $bq_lite = Google::BigQuery::Lite->new(
        client_email => $client_email,
        private_key_file => $private_key_file,
        project_id => $project_id,
    );
    # create a dataset
    my $dataset_id = 'usa_names';
    $bq_lite->use_dataset($dataset_id);

    # define field name and json schema
    my $field_definitions = [
        { name => 'id', schema => +{
            type => ['integer'],
        } },
        { name => 'int_value', schema => +{
            type => ['integer'],
        } },
        { name => 'num_value', schema => +{
            type    => ['number'],
            minimum => 0.0,
            maximum => 100.0,
        } },
        { name => 'array_int', schema => +{
            type     => [ 'array', 'null' ],
            maxItems => 10,
            items    => +{
                type => 'integer',
            }
        } },
    ];

    my $row = $bq_lite->selectrow_arrayref(
        query => "SELECT * FROM $table_id ORDER BY id LIMIT 1",
        useLegacySql => 0,
        field_definitions => $field_definitions,
    );
    # { id => 123, int_value => 1234, num_value => 123.4, array_int => [ 1, 12, 123 ] }

    my $rows = $bq_lite->selectall_arrayref(
        query => "SELECT * FROM $table_id ORDER BY id",
        useLegacySql => 0,
        field_definitions => $field_definitions,
    );
    # [{ id => 123, int_value => 1234, num_value => 123.4, array_int => [ 1, 12, 123 ] }]

# DESCRIPTION

Google::BigQuery::Lite is ...

- `$field_definitions`

    dictionary of **field name** and **JSON Schema**.

        my $field_definitions = [
            # id is not null, must be integer
            { name => 'id', schema => +{
                type => ['integer'],
            } },
            # int_value is not null, must be integer
            { name => 'int_value', schema => +{
                type => ['integer'],
            } },
            # num_value is not null, must be number, minimum 0.0, maximum 100.0
            { name => 'num_value', schema => +{
                type    => ['number'],
                minimum => 0.0,
                maximum => 100.0,
            } },
            # array_int is nullable array of integer, max items 10
            { name => 'array_int', schema => +{
                type     => [ 'array', 'null' ],
                maxItems => 10,
                items    => +{
                    type => 'integer',
                }
            } },
        ];

# LICENSE

Copyright (C) yujiorama.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

yujiorama <yujiorama+github@gmail.com>
