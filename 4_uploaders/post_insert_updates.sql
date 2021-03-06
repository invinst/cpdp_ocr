--inserting data from csvs from lawsuit. 
--file is not always included and format is always different, so one-offs are required.

\COPY cr_batch_data (control_number, prod_begin, prod_end, prod_begin_attachment, prod_end_attachment, filename, filepath, placeholder) FROM '../0_setup/input/Green, C. 2019.09.03 Production_export.csv' DELIMITER ',' CSV ;
\COPY cr_batch_data (prod_begin, prod_end, filename, log_number, placeholder) FROM '../0_setup/input/Green 2019.12.30 Production.csv' DELIMITER ',' CSV ;
\COPY cr_batch_data (prod_begin, prod_end, log_number) FROM '../0_setup/input/Green 2019.12.02 Production.csv' DELIMITER ',' CSV ;

update cr_pdfs set cr_id = split_part(log_number, '_', 2)::int from cr_batch_data bd where log_number like '%LOG%' and cr_pdfs.filename = concat(bd.prod_begin, '.pdf') ;
update cr_pdfs set cr_id = (regexp_split_to_array(filename, '[_ .]'))[2]::integer where filename like 'LOG_%' ;
update cr_pdfs set cr_id = split_part(split_part(bd.filename, '_', 2), '.', 1)::int from cr_batch_data bd where bd.filename like '%LOG%' and cr_pdfs.filename = concat(bd.prod_begin, '.pdf') ;
