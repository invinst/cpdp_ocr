This task takes as input a set of page-classifications. It downloads files and
pulls out the specified report types into their own PDFs. Finally, it takes the
resulting PDF, which should at that point be just one type of document with
possibly many pages, and extracts structured data into a JSON file.

Right now, only the **Original Case Incident Report** report type has been
implemented.

Structured extraction into JSON files only includes select fields, it is not an
exhaustive representation of the original PDF.
