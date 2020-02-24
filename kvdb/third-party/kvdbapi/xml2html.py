#!/usr/bin/python
#coding=utf8

import sys
import libxml2
import libxslt

class compoundXML:
    def __init__(self):
        self._result=None
        self._xsl=None
        self._xml=None
    
    def do(self,xml_file_name,xsl_file_name='gtest.xsl'):
        self._xml = libxml2.parseFile(xml_file_name)
        if self._xml ==None:
            return 0
        styledoc = libxml2.parseFile(xsl_file_name)
        if styledoc == None:
            return 0
        self._xsl = libxslt.parseStylesheetDoc(styledoc)
        if self._xsl == None:
            return 0
        self._result = self._xsl.applyStylesheet(self._xml, None)
    def get_xml_doc(self):
        return self._result

    def get_translated(self):
        return self._result.serialize('UTF-8')

    def save_translated(self, file_name): 
        self._xsl.saveResultToFilename(file_name, self._result, 0) 

    def release(self): 
        ''' 
        this function must be called in the end. 
        ''' 
        self._xsl.freeStylesheet() 
        self._xml.freeDoc() 
        self._result.freeDoc() 
        self._xsl = None 
        self._xml = None 
        self._result = None 

def xml2html(xml_file):
    test=compoundXML()
    test.do(xml_file)
    test.save_translated(xml_file+'.html')
    test.release()

if __name__ =='__main__':
    filename=sys.argv[1];
    test=compoundXML()
    test.do(filename)
    #print test.get_translated()
    test.save_translated(filename+'.html')
    test.release()
