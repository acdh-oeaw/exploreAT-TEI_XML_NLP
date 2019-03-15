#!/usr/bin/env python
# coding: utf-8

import os
import sys
import time
import pickle
import string
from datetime import datetime
from datetime import timedelta
import collections
from pprint import pprint
from io import StringIO
import pathlib
import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from xml.dom import minidom
from xml.etree import ElementTree as ET
#from lxml import etree as ET #Supports xpath syntax

datapath = './TEI-XML-2018/'
onlydirs = [f for f in os.listdir(datapath) if os.path.isdir(os.path.join(datapath,f)) and f.startswith('bel')]
onlyfiles = []
for folder in onlydirs:
    for filex in [f for f in os.listdir(os.path.join(datapath, folder)) if os.path.isfile(os.path.join(datapath, folder, f))                                        and not f.startswith('.')]:
        onlyfiles.append('{}{}/{}'.format(datapath, folder, filex))


print('Files in the folder:')
for i, w in enumerate(onlyfiles):
    print(i+1, '--' ,w)

t0 = time.time()

columns=['filename',
         'entry_id',
         'date',
         'usg_corresp', 
         'simple_placeName',
         'orig_placeName',
         'placeName_id',
         'Bundesland',
         'Bundesland_idno',
         'Großregion',
         'Großregion_idno',
         'Kleinregion',
         'Kleinregion_idno',
         'Gemeinde',
         'Gemeinde_idno',
         'Ort',
         'Ort_idno',
         'hauptlemma_unique',
         'hauptlemma_orig',
         'hauptlemma_normalized',
         'nebenlemma_unique',
         'nebenlemma_orig',
         'nebenlemma_normalized',
         'verweise',
         'archiv',
         'quelle',
         'quelleBearbeitet',
         'seite',
         'fragebogenNummer',
         'paragraph',
         'bibl',
         'tustep',
         'pos',
         'etym',
         'note_notabene',
         'note_anmerkung',
         'note_diverse',
         'cit_type',
         'cit_quote',
         'cit_def',
         'cit_interp',
         'cit_ref',
         'cit_pRef',
         'cit_note',
         'cit_re',
         'certainty',
        ]

df_lemmas = pd.DataFrame()

for fileid, teixmlfile in enumerate(onlyfiles):
    list_records = []
    #tree = ET.parse(teixmlfile)
    #root = tree.getroot()
    root = ET.parse(teixmlfile).getroot()
    for entry in root.iter('{http://www.tei-c.org/ns/1.0}entry'):

        record = {'filename': teixmlfile,
                  'entry_id': None,
                  'date': None,
                  'usg_corresp': None,
                  'simple_placeName': None,
                  'orig_placeName': None,
                  'placeName_id': None,
                  'Bundesland': None,
                  'Bundesland_idno': None,
                  'Großregion': None,
                  'Großregion_idno': None,
                  'Kleinregion': None,
                  'Kleinregion_idno': None,
                  'Gemeinde': None,
                  'Gemeinde_idno': None,
                  'Ort': None,
                  'Ort_idno': None,
                  'hauptlemma_unique': None,
                  'hauptlemma_orig': None,
                  'hauptlemma_normalized': None,
                  'nebenlemma_unique': None,
                  'nebenlemma_orig': None,
                  'nebenlemma_normalized': None,
                  'verweise': None,
                  'archiv': None,
                  'quelle': None,
                  'quelleBearbeitet': None,
                  'seite': None,
                  'fragebogenNummer': None,
                  'paragraph': None,
                  'bibl': None,
                  'tustep': [],
                  'pos': None,
                  'etym': [],
                  'note_notabene': None,
                  'note_anmerkung': None,
                  'note_diverse': None,
                  'cit_type': None,
                  'cit_quote': [],
                  'cit_def': [],
                  'cit_interp': [],
                  'cit_ref': [],
                  'cit_pRef': [],
                  'cit_note': [],
                  'cit_re':[],
                  'certainty':[],
                 }

        record['entry_id'] = entry.attrib['{http://www.w3.org/XML/1998/namespace}id']
        
        for child in entry.iter():
            level = 'simple_placeName'
            if child.tag == '{http://www.tei-c.org/ns/1.0}usg':
                if 'corresp' in child.attrib:
                    record['usg_corresp'] = child.attrib['corresp']
                for subchild in child.iter():
                    if 'type' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}placeName'                            and subchild.attrib['type'] == 'orig':
                        record['orig_placeName'] = subchild.text

                    elif 'ref' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}listPlace':
                        record['placeName_id'] = subchild.attrib['ref']

                    elif 'type' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}place'                             and subchild.attrib['type'] == 'Bundesland':
                        level = 'Bundesland'

                    elif 'type' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}place'                             and subchild.attrib['type'] == 'Großregion':
                        level = 'Großregion'

                    elif 'type' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}place'                             and subchild.attrib['type'] == 'Kleinregion':
                        level = 'Kleinregion'

                    elif 'type' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}place'                             and subchild.attrib['type'] == 'Gemeinde':
                        level = 'Gemeinde'

                    elif 'type' in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}place'                             and subchild.attrib['type'] == 'Ort':
                        level = 'Ort'

                    if 'type' not in subchild.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}placeName':
                        record[level] = subchild.text
                    elif 'type' in child.attrib                             and subchild.tag == '{http://www.tei-c.org/ns/1.0}idno':
                        record['{}_idno'.format(level)] = subchild.text

            elif child.tag == '{http://www.tei-c.org/ns/1.0}form'                         and 'type' in child.attrib:
                if child.attrib['type'] == 'hauptlemma':
                    for subchild in child.iter():
                        if subchild.tag == '{http://www.tei-c.org/ns/1.0}orth':
                            if 'type' not in subchild.attrib:
                                record['hauptlemma_unique'] = subchild.text
                            elif 'type' in subchild.attrib and subchild.attrib['type'] == 'orig':
                                record['hauptlemma_orig'] = subchild.text
                            elif 'type' in subchild.attrib and subchild.attrib['type'] == 'normalized':
                                record['hauptlemma_normalized'] = subchild.text
                elif child.attrib['type'] == 'nebenlemma':
                    for subchild in child.iter():
                        if subchild.tag == '{http://www.tei-c.org/ns/1.0}orth':
                            if 'type' not in subchild.attrib:
                                record['nebenlemma_unique'] = subchild.text
                            elif 'type' in subchild.attrib and subchild.attrib['type'] == 'orig':
                                record['nebenlemma_orig'] = subchild.text
                            elif 'type' in subchild.attrib and subchild.attrib['type'] == 'normalized':
                                record['nebenlemma_normalized'] = subchild.text       
                elif child.attrib['type'] == 'lautung':
                    for subchild in child.iter():
                            if 'notation' in subchild.attrib and subchild.attrib['notation'] == 'tustep':
                                record['tustep'].append(subchild.text)

            elif child.tag == '{http://www.tei-c.org/ns/1.0}pos':
                record['pos'] = child.text
            
            elif child.tag == '{http://www.tei-c.org/ns/1.0}date':
                record['date'] = child.text
                
            elif child.tag == '{http://www.tei-c.org/ns/1.0}note':
                if 'type' in child.attrib and child.attrib['type'] == 'notabene':
                    record['note_notabene'] = subchild.text
                elif 'type' in child.attrib and child.attrib['type'] == 'anmerkung':
                    record['note_anmerkung'] = child.text
                elif 'type' in child.attrib and child.attrib['type'] == 'diverse':
                    record['note_diverse'] = child.text
                elif 'type' in child.attrib:
                    print('new note type ', child.attrib['type'] )

            elif child.tag == '{http://www.tei-c.org/ns/1.0}cit':
                for subchild in child.iter():
                    if subchild.tag == '{http://www.tei-c.org/ns/1.0}cit':
                        record['cit_type'] = subchild.attrib['type']
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}quote':
                        record['cit_quote'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}def':
                        record['cit_def'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}interp':
                        record['cit_interp'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}ref':
                        record['cit_ref'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}pRef':
                        record['cit_pRef'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}note':
                        record['cit_note'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}re':
                        record['cit_re'].append(subchild.text)
                    elif subchild.tag == '{http://www.tei-c.org/ns/1.0}certainty':
                        record['certainty'].append(subchild.text)
                    else:
                        print('new cit tag ', subchild.tag)                
                    
            elif child.tag == '{http://www.tei-c.org/ns/1.0}ref':
                if child.attrib['type'] == 'verweise':
                    record['verweise'] = child.text
                elif child.attrib['type'] == 'archiv':
                    record['archiv'] = child.text
                elif child.attrib['type'] == 'quelle':
                    record['quelle'] = child.text
                elif child.attrib['type'] == 'quelleBearbeitet':
                    record['quelleBearbeitet'] = child.text
                elif child.attrib['type'] == 'seite':
                    record['seite'] = child.text
                elif child.attrib['type'] == 'fragebogenNummer':
                    record['fragebogenNummer'] = child.text
                elif child.attrib['type'] == 'paragraph':
                    record['paragraph'] = child.text
                elif child.attrib['type'] == 'bibl':
                    record['bibl'] = child.text
                else:
                    print('new ref type --> ', child.attrib['type'])
            
            elif child.tag == '{http://www.tei-c.org/ns/1.0}etym':
                for subchild in child.iter():
                    record['etym'].append(subchild.text)
                    
            list_records.append(record)
             
    df_lemmas = df_lemmas.append(list_records, ignore_index=True)
    print('Processing time is {0:.2f} minutes for file {1}'.format(((time.time() - t0) / 60), fileid))


df_lemmas.to_pickle('./df_lemmas.pkl')