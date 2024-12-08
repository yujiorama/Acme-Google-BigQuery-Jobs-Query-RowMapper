requires 'JSON';
requires 'Valiemon';
requires 'Google::BigQuery';

on configure => sub {
    requires 'Module::Build::Tiny', '0.035';
    requires 'perl', '5.008_001';
};

on develop => sub {
    requires 'Data::Printer';
    requires 'Perl::Tidy';
};

on test => sub {
    requires 'Test2::V0';
};
