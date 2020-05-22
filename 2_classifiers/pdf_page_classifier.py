#!/usr/bin/python3

from skimage.measure import compare_ssim
import psycopg2
import os
import re
import csv
import cv2

from strsimpy.normalized_levenshtein import NormalizedLevenshtein

nlevenstein = NormalizedLevenshtein()

def get_image(path, norm_size=True, norm_exposure=False):
    #img = #Image.open(path)
    img = cv2.imread(path)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return img

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

def crid_from_txt_file(path):
    #grabs the CPD ID from the bottom of the screen. 
    #cases is often in the same place, on the last line, or soon before
    with open(path, 'r') as txt_fh:
        lines = txt_fh.readlines()

    for line in lines[::-1]:
        line = line.strip()
        if re.search('CPD {,2}00[0-3][0-9]{4,6}', line):
            sanitized_line = re.sub('^.*CPD 00', 'CPD 00', line)
            sanitized_line = ' '.join(re.split('[^CPD0-9]', sanitized_line)[:2])
            return(sanitized_line) #found "CPD ####### "

    print("Bad page at: ", path)

def identify_pdf_cpd_ids(pdf_name, begin, end):
    #iterates through pages in each pdf dir
    assert type(begin) == int and type(end) == int
    
    pages_cpd_ids = []
    pdf_dir = '{}/{}'.format('./input', pdf_name)
    pdf_files = sorted(os.listdir(pdf_dir))

    txt_files = [i for i in pdf_files if i.endswith('.txt')]
    for txt_file in txt_files:
        page_num = int(os.path.basename(txt_file).split('.')[-2])
        pages_cpd_id = crid_from_txt_file('{}/{}'.format(pdf_dir, txt_file))
        pages_cpd_ids.append((page_num, pages_cpd_id))

    differents = set(range(begin, end+1)).difference([int(i[1].split()[-1]) for i in pages_cpd_ids if i[1]])
    if len(differents) == 1:
        print("Different of 1 for ", pdf_name)
        new_pages_cpd_ids = []
        for page_num, cpd_id in pages_cpd_ids:
            if not cpd_id:
                cpd_id = list(differents)[0]
                cpd_id = 'CPD {}'.format(''.join(['0' for i in range(0, (7-len(str(cpd_id))+1))]))

            new_pages_cpd_ids.append((page_num, cpd_id))

        pages_cpd_ids = new_pages_cpd_ids
        differents = list(set())

    return(pages_cpd_ids, differents)

def identify_doc_type(pdf_name, page_num=1, filename=None):
    doc_type = None
    filepath = '{}/{}'.format('./input', pdf_name)

    startswith_map = (
        ('FACE SHEET (Notification','Face Sheet'),
        ('AUTO CR - LOG  SUMMARY', 'Log Summary Report'),
        ('Incident Finding / Overall Case Finding', 'Summary Report'),
        ('View AA Sheet', 'A&A Sheet'),
        ('SUMMARY REPORT DIGEST', 'Summary Report Digest'),
        ('REQUEST FOR CRIME', 'Request for Crime Scene / Evidence Photographs'),
        ('ADMINISTRATIVE PROCEEDINGS RIGHTS', 'Administrative Proceedings Rights'),
        ('Incident Detail Page','Incident Detail Page'),
        ('MEDIATION AGREEMENT', 'Mediation Agreement'),
        ('NOTICE OF ALCOHOL', 'Notice of Alcohol And Drug Testing Following A Firearms Discharge Incident'),
        ('SURVEILLANCE REPORT', 'Surveillance Report'),
        ('SWORN AFFIDAVIT', 'Sworn Affidavit'),
        ('REQUEST TO BYPASS', 'Request to Bypass Command Channel Review'),
        ('AUTHORIZATION FOR RELEASE', 'Authorization for Release'),
        ('REQUEST FOR INTERVIEW/STATEMENT/REPORT', 'Request for Interview/Statement/Report'),
        ('DIGITALLY RECORDED DATA', 'Digitally Recorded Data Viewing/Hold/Duplication Request'),
        ('Complaint Log Investigation', 'Complaint Log Investigation'),
        ('SEARCH WARRANT DATA', 'Search Warrant Data'),
        ('Crime Scene Report', 'Crime Scene Report'),
        ('TASER Information', 'TASER Report'),
        ('Mugshot(s) for CB', 'Mugshot(s) for CB'),
        ('PHOTOGRAPHIC EVIDENCE COVERSHEET', 'Photographic Evidence Coversheet'),
        ('USPS.com® - Track', 'USPS.com Tracking'),
        ('USPS - ZIP Code Lookup - Search By', 'USPS Zip Code Lookup'),
        ('CASE SUPPLEMENTARY REPORT', 'Case Supplementary Report'),
        ("OFFICER'S BATTERY REPORT", "Officer's Battery Report"),
        ('NOTIFICATION OF CHARGES/ALLEGATIONS', 'Notification of Charges/Allegations'),
        ('PROPERTY INVENTORY', 'Property Inventory'),
        ("WEB Complaint Detail", 'WEB Complaint Detail'),
        ('WAIVER OF COUNSEL/REQUEST', 'Waiver of Counsel/Request'),
        ("INVESTIGATOR’S CASE LOG", "Investigator's Case Log"),
        ('SWORN AFFIDAVIT ', 'Sworn Affidavit'),
        ('SWORN AFFIDAVIT FOR', 'Sworn Affidavit'),
        ('TACTICAL RESPONSE REPORT', 'Tactical Reponse Report'),
        ('ORIGINAL CASE INCIDENT','Face Sheet'),
        ('Chicago Police Department - ARREST Report', 'ARREST Report'),
        ('WATCH COMMANDER/OCIC REVIEW', 'Watch Commander/OCIC Review'),
        ('EVENT HISTORY TABLE', 'Event History Table'),
        ('A FIREARMS DISCHARGE INCIDENT', 'Notice of Alcohol And Drug Testing Following a Firearms Discharge Incident'),
        ('DRUG TEST SPECIMEN AFFIDAVIT', 'Drug Test Specimen Affidavit'),
        ('ALTERNATE COLLECTION RECEIPT', 'Random Drug Testing Unit Alternate Collection Receipt'),
        ('RANDOM DRUG TESTING UNIT', 'Random Drug Testing Unit Alternate Collection Receipt'),
        ('Event Query', 'Event Query'),
        ('HOME. A & A Sheet', 'A&A Sheet'),
        ('A & A Sheet', 'A&A Sheet'),
        ('PMIS GPS 001 Report', 'PMIS GPS 001 Report'),
        ('Data Warehouse Address of Arrest Search', 'CLEAR Data Warehouse Arrestee Name Check'),
        ('CHICAGO POLICE DEPARTMENT - Digital Mugshot System', 'Digital Mugshot System Screenshot'),
        ('Investigate Incident - Involved Parties', 'Investigate Incident Involved Parties'),
        ('Print Unit Inventory Full Listing', 'Vehicle Inventory Report'),
        ('SENDER: COMPLETE THIS SECTION ', 'Certified Mail'),
        ('CERTIFIED MAIL', 'Certified Mail'),
        ('PODS > Video Retrieval', 'PODS Video Retrieval'),
        ("SUPERVISOR'S MANAGEMENT LOG", 'Supervisor Management Log'),
        ("Employee Statement of Earnings", 'Employment Statement of Earnings'),
        ("OVERTIME/COMPENSATORY TIME REPORT", 'Overtime/Compensatory Time Report'),
        ("SURVEILLANCE VIDEO", "Surveillance Video"),
        ("VICTIM INFORMATION NOTICE", "Victim Information Notice"),
        ("eTrack Inventory Item Inquiry", "eTrack Inventory Item Inquiry"),
        ("Narcotic & Gang Investigation Section Supplementary Report", "Narcotic & Gang Investigation Supplementary Report"),
        ("Incident Check From the Crimes Tables", "Incident Check From the Crimes Tables"),
        ("Fax Transmission Report", "Fax Transmission Report"),
        ("Fax Broadcast Report", "Fax Broadcast Report"),
        ("Personnel Check Sorted by", "Personnel File Check Results"),
        ("GENERAL PROGRESS REPORT", "General Progress Report"),
        ("REQUEST FOR REVIEW OF DISCIPLINE", "Request for Review of Discipline"),
        ("REQUEST FOR POLICE BOARD REVIEW/ELECT TO FILE A", "Request for Police Board Review to File Grievance"),
        ("Chicago Police Department - Incident Report", "Incident Report")
    )

    tail_startswith_map = (
            ('AUTO CR - LOG SUMMARY', 'Summary Report'),
            ('VICTORIA COURT REPORTING SERVICE, INC.', 'Digitally Recorded Statement'),
            ('http://mugshot.chicagopolice.local/mod_iad/', 'Digital Mugshot System Screenshot'),
            ('CPD-11.377', 'CPD-11.377'),
            ('CPD-11.377','CPD-11.377'),
            ('CPD-44.126','CPD-44.126'),
            ('CPD-44.115','CPD-44.115'),
            ('CPD-41.703','CPD-41.703'),
            ('CPD-11.483','CPD-11.483'),
            ('CPD-34.523','CPD-34.523'),
            ('CPD-44.223','CPD-44.223'),
            ('CPD-11.451','CPD-11.451'),
            ('CPD-44.105','CPD-44.105'),
            ('CPD-44.106','CPD-44.106'),
            ('CPD-62.441','CPD-62.441'),
            ('CPD-11.608','CPD-11.608'),
            ('CPD-44.104','CPD-44.104'),
            ('CPD-33.713','CPD-33.713'),
            ('CPD-23.122','CPD-23.122'),
            ('CPD-65.224','CPD-65.224'),
            ('CPD-44.103','CPD-44.103'),
            ('CPD-44.402','CPD-44.402'),
            ('CPD-44.509','CPD-44.509'),
            ('CPD-11.455','CPD-11.455'),
            ('CPD-62.111','CPD-62.111'),
            ('CPD-62.130','CPD-62.130'),
            ('CPD-11.454','CPD-11.454'),
            ('https://webmail.chicagopolice.org','CPD Webmail Email'),
            ('https://chris.chicagopolice.org/pls/clear/law_rapsheet_cpd','Criminal History Report'),
            ('This e-mail, and any attachments thereto, is intended only for use by the addressee', 'Outlook Email'),
        )

    #Within each map's list, all fields must be matched on the left side, or at least within a close string distance.
    #First entry has highest priority
    leftside_maps = (
            (['TO:',
                'FROM:',
                'SUBJECT:',
                'ALLEGATION:',
       ], 'Allegation Email'),
             (['TO:',
                'FROM:',
                'SUBJECT:',
       ], 'email'),
             (["DRIVER'S COPY - See Reverse for Court Location", "IN THE CIRCUIT COURT OF COOK COUNTY",], 'Traffic Violation'),
             (['Extension History', 'Current Allegations', 'Situations (Allegation Details)', 'Status History'], 'Summary Report'),
            (['MEDICAL RECORDS',
                'Spec. Type:',
                'Result Name',
                '*** DRAFT - See performing lab for Final report'], 'medical records'),
        (['EMERGENCY MANAGEMENT & COMMUNICATIONS', 'EXPLANATION FOR MIS GPS 001 REPORT', 'Explanation for MIS GPS 001 Report'], 'Explanation for MIS GPS 001 Report'),
        (['Legend', 'CPD Vehicle Location', 'Map Created'], 'OEMC GPS Map'),
        (['VALLEY WEST COMMUNITY HOSPITAL', 'DIAGNOSTIC IMAGING DEPARTMENT', 'Signed Report'], 'Hospital Imaging Report'),
        (['are not intended as complete care. It is important', 'symptoms become worse or you do not improve as'], 'Emergency Department Discharge Instructions'),
        (['CHICAGO POLICE DEPARTMENT', 'CLEARMAP', 'TOTAL PODS'], 'CLEARMAP POD Map'),
        (['BUREAU OF INTERNAL AFFAIRS', 'INVESTIGATIONS DEVISION', 'Statement Of:', 'Statement taken at:', 'Questioned by:', 'Date and Time'], 'Statement'),
        (['TASER Information', 'Recorded Firing Data', 'Recorded X26 Time Changes'], 'TASER Report'),
        (['Case:', 'DEFENDANT NAME', 'ATTORNEY INFORMATION', 'ENTER=CONT'], 'Court Case Report'),
        (['Map Output', 'Violence & Drug Related Crime',], 'Violence & Drug Related Crime Map'),
        (['U.S. Postal Service', 'CERTIFIED MAIL', 'Domestic Mail Only;', 'For delivery information visit our website'], 'Certified Mail Receipt'),
        (['REASON FOR WORKING OVERTIME', 'OVERTIME AUTHORIZED BY', 'COURT NOTIFICATION RECORD NO', 'EXPLAIN ASSIGNMENT'], 'Overtime/Compensatory Time Report'),
        (['There is somone to answer', 'The fullest investigation of this complaint is possible'], 'IPRA Investigation'),
        (['TO REPORT ADDITIONAL INFORMATION', 'COPY OF THE REPORT', 'MAKE THE RIGHT CALL', 'CHICAGO ALTERNATIVE POLICING'], 'Victim Information Notice'),
        (['PERSONS PRESENT NOT ARRESTED', 'EVIDENCE RECOVERED', 'TOTAL WEIGHT & STREET VALUE', 'EVIDENCE OFFICER'], 'Narcotic & Gang Investigation Supplementary Report'),
        (['We have nonetheless begun to investigate', 'Please contact me at'], 'Narcotic & Gang Investigation Supplementary Report'),
        (['HS: Host Send', 'HR: Host Receive', 'MS: Mailbox Save', 'MP: Mailbox Print'], 'Fax Report'),
    )

    skippable_lines = ['CITY OF CHICAGO']

    try:
        fp = '{}/{}.{}.txt'.format(filepath, pdf_name, page_num)
        with open(fp, 'r') as fh:
            lines = [l for l in fh.readlines() if re.search('[a-zA-Z]{3,}', l) and l not in skippable_lines]

    except:
        print("MISSING FILES:", pdf_name)
        return

    if not lines:
        return

    #key mapping
    for startswith_text, normalized_docname in startswith_map:
        startswith_text = startswith_text.lower()
        for line in lines[:2]:
            trimmed_start = line[:len(startswith_text)].lower()

            if line.startswith(startswith_text):
                return normalized_docname
            elif nlevenstein.distance(trimmed_start.lower(), startswith_text.lower()) < .15:
                return normalized_docname

    #check end of document #TODO: generalize this.
    for startswith_text, normalized_docname in tail_startswith_map:
        startswith_text = startswith_text.lower()
        for line in lines[-2:]:
            trimmed_start = line[:len(startswith_text)].lower()

            if line.startswith(startswith_text):
                return normalized_docname
            elif nlevenstein.distance(trimmed_start.lower(), startswith_text.lower()) < .15:
                return normalized_docname

    #pop down stack of known and in-order line beginnings 
    #if all lines found, leftsides should be empty.
    for leftsides, normalized_docname in leftside_maps:
        curr_left = leftsides.pop(0)

        for line in lines:
            trimmed_start = line[:len(curr_left)]
            if nlevenstein.distance(trimmed_start, curr_left) <= .125 or line.startswith(curr_left):
                if not leftsides:
                    #emails have their own doc_types
                    if normalized_docname == 'Email':
                        email_type = sanitize_subject(line)
                        return email_type

                    return normalized_docname

                curr_left = leftsides.pop(0)

        if not leftsides:
            return normalized_docname

    return None

def tag_pdf(pdf_id, pdf_name):
    ocrd_path = '{}/{}'.format('./input', pdf_name)
    try:
        fps = os.listdir(ocrd_path)
    except:
        print("Directory for {} not found. Skipping".format(ocrd_path))
        return

    txt_files = [p for p in fps if p.endswith('.txt')]
    img_files = [p for p in fps if p.endswith('.png')]

    tagged_pages = []
    for txt_file in txt_files:
        try:
            page_num = int(txt_file.split('.')[-2])
        except:
            print(page_num)
        doc_type = identify_doc_type(pdf_name, page_num) 

        if doc_type:
            print(page_num, doc_type)

        tagged_pages.append((pdf_name, pdf_id, page_num, doc_type))

    return tagged_pages

if __name__ == '__main__':
    conn = pg_conn()
    curs = conn.cursor()

    sqlstr = "SELECT id, filename FROM cr_pdfs"
    pdfs = curs.execute(sqlstr)
    pdfs = curs.fetchall()

    pdf_crids = {}
    tagged_pdfs = []
    tagged_pages = []
    
    tag_params = []
    
    from multiprocessing import Pool
    pool = Pool(processes=30)

    results = pool.starmap(tag_pdf, pdfs, chunksize=8)
   
    sqlstr = "UPDATE cr_pdf_pages set page_classification = %s WHERE pdf_id = %s and page_num = %s"
    for pdf_results in results:
        for _, pdf_id, page_num, classification in pdf_results:
            curs.execute(sqlstr, (classification, pdf_id, page_num))

        conn.commit()
