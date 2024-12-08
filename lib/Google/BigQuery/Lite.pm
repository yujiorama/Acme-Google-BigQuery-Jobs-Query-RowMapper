package Google::BigQuery::Lite;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use parent 'Google::BigQuery::V2';

use Valiemon;
use JSON qw(decode_json);

use constant DEFAULT_LOCATION => 'us-west-1';

# https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#queryrequest
use constant QUERY_REQUEST_SCHEMA => <<'JSON';
    {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "definitions": {
            "types": {
                "DatasetReference": {
                    "type": "object",
                    "required": [
                        "datasetId"],
                    "properties": {
                        "datasetId": {
                            "type": "string"
                        },
                        "projectId": {
                            "type": "string"
                        }
                    }
                },
                "QueryParameterType": {
                    "type": "object",
                    "required": [
                        "type"],
                    "properties": {
                        "type": {
                            "type": "string"
                        },
                        "arrayType": {
                            "$ref": "#/definitions/types/QueryParameterType"
                        },
                        "structTypes": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": [
                                    "type"],
                                "properties": {
                                    "name": {
                                        "type": "string"
                                    },
                                    "description": {
                                        "type": "string"
                                    },
                                    "type": {
                                        "$ref": "#/definitions/types/QueryParameterType"
                                    }
                                }
                            }
                        },
                        "rangeElementType": {
                            "$ref": "#/definitions/types/QueryParameterType"
                        }
                    }
                },
                "QueryParameterValue": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "value": {
                            "type": "string"
                        },
                        "arrayValues": {
                            "$ref": "#/definitions/types/QueryParameterValue"
                        },
                        "structValues": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": {
                                "$ref": "#/definitions/types/QueryParameterValue"
                            }
                        },
                        "rangeValues": {
                            "type": "object",
                            "required": [],
                            "properties": {
                                "start": {
                                    "$ref": "#/definitions/types/QueryParameterValue"
                                },
                                "end": {
                                    "$ref": "#/definitions/types/QueryParameterValue"
                                }
                            }
                        }
                    }
                },
                "QueryParameter": {
                    "type": "object",
                    "required": [
                        "name",
                        "parameterType",
                        "parameterValue"],
                    "properties": {
                        "name": {
                            "type": "string"
                        },
                        "parameterType": {
                            "$ref": "#/definitions/types/QueryParameterType"
                        },
                        "parameterValue": {

                        }
                    }
                },
                "DataFormatOptions": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "useInt64Timestamp": {
                            "type": "boolean"
                        }
                    }
                }
            }
        },
        "type": "object",
        "required": [
            "query",
            "location"],
        "properties": {
            "kind": {
                "type":  "string" },
            "query": {
                "type":  "string" },
            "maxResults": {
                "type":  "integer" },
            "defaultDataset": {
                "$ref": "#/definitions/types/DatasetReference"
            },
            "timeoutMs": {
                "type":  "integer" },
            "dryRun": {
                "type":  "boolean" },
            "preserveNulls": {
                "type":  "boolean" },
            "useQueryCache": {
                "type":  "boolean" },
            "useLegacySql": {
                "type":  "boolean" },
            "parameterMode": {
                "type":  "string" },
            "queryParameters": {
                "$ref": "#/definitions/types/QueryParameter"
            },
            "location": {
                "type":  "string" },
            "formatOptions": {
                "$ref": "#/definitions/types/DataFormatOptions"
            },
            "maximumBytesBilled": {
                "type":  "string" },
            "requestId": {
                "type":  "string" },
            "createSession": {
                "type":  "string",
                "enum": [
                    "JOB_CREATION_MODE_UNSPECIFIED",
                    "JOB_CREATION_REQUIRED",
                    "JOB_CREATION_OPTIONAL"
                ]
            }
        }
    }
JSON

sub new {
    my ( $class, %args ) = @_;
    my $self = $class->SUPER::new(%args);
    return bless $self, $class;
}

sub selectrow_array {
    my ( $self, %args ) = @_;

    my $selectall = $self->selectall_arrayref( %args, maxResults => 1 );
    return $selectall->[0];
}

sub _assert_query_request {
    my ( $self, $query_request ) = @_;

    my $validator = Valiemon->new( decode_json( QUERY_REQUEST_SCHEMA() ) );

    my ( undef, $validation_error ) = $validator->validate($query_request);

    if ( defined $validation_error ) {
        die sprintf '%s: expected=%s actual=%s error=%s',
          'QueryRequest',
          $validation_error->expected,
          $validation_error->actual,
          $validation_error->as_message,
          ;
    }
}

sub selectall_arrayref {
    my ( $self, %args ) = @_;
    my $query      = $args{query};
    my $project_id = $args{project_id} // $self->{project_id};
    my $dataset_id = $args{dataset_id} // $self->{dataset_id};
    unless ($query) {
        warn "no query\n";
        return 0;
    }
    unless ($project_id) {
        warn "no project\n";
        return 0;
    }
    my $content = {
        query    => $query,
        location => $args{location} // DEFAULT_LOCATION(),
    };

    # option
    if ( defined $dataset_id ) {
        $content->{defaultDataset}{projectId} = $project_id;
        $content->{defaultDataset}{datasetId} = $dataset_id;
    }
    for my $key (qw(maxResults timeoutMs maximumBytesBilled)) {
        if ( defined $args{$key} ) {
            $content->{$key} = $args{$key};
        }
    }
    for my $key (qw(dryRun useQueryCache useLegacySql)) {
        if ( defined $args{$key} ) {
            $content->{$key} = $args{$key} ? 'true' : 'false';
        }
    }
    $self->_assert_query_request($content);

    my $response = $self->request(
        resource => 'jobs',
        method   => 'query',
        content  => $content
    );
    $self->{response} = $response;
    if ( defined $response->{error} ) {
        warn $response->{error}{message};
        return 0;
    }

    return [
        map { $self->_to_hashref( $_->{f} // [], $args{field_definitions} ) }
          @{ $response->{rows} // [] } ];
}

sub _to_hashref {
    my ( $self, $fields, $field_definitions ) = @_;

    my $ret = +{};
    for (
        my $field_index = 0 ;
        $field_index < scalar @{$fields} ;
        $field_index++
      )
    {
        my $field_value = $fields->[$field_index]->{v};

        if ( ref($field_value) eq 'ARRAY' ) {
            $field_value = [ map { $_->{v} } @{$field_value} ];
        }

        my $field_definition = $field_definitions->[$field_index] // +{};

        if ( my $schema = $field_definition->{schema} ) {
            my $validator = Valiemon->new($schema);

            my ( undef, $validation_error ) =
              $validator->validate($field_value);

            if ( defined $validation_error ) {
                warn sprintf '%s: expected=%s actual=%s error=%s',
                  $field_definition->{name},
                  $validation_error->expected,
                  $validation_error->actual,
                  $validation_error->as_message,
                  ;

                $field_value = undef;
            }
        }

        $ret->{ $field_definition->{name} // $field_index } = $field_value;
    }

    return $ret;
}

1;
__END__

=encoding utf-8

=head1 NAME

Google::BigQuery::Lite - It's new $module

=head1 SYNOPSIS

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

=head1 DESCRIPTION

Google::BigQuery::Lite is ...

=over 4

=item C<< $field_definitions >>

dictionary of B<field name> and B<JSON Schema>.

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

=back

=head1 LICENSE

Copyright (C) yujiorama.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

yujiorama E<lt>95338339+yokazawa@users.noreply.github.comE<gt>

=cut

