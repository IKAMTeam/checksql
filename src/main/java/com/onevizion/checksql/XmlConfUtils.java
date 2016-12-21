package com.onevizion.checksql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.onevizion.checksql.exception.UnexpectedException;
import com.onevizion.checksql.exception.XmlConfException;

public class XmlConfUtils {

    public static Document getDoc(String fileName) {
        InputStream file = null;
        try {
            file = new FileInputStream(new File(fileName));
        } catch (FileNotFoundException e) {
            throw new UnexpectedException("Cannot read XML document", e);
        }

        Document doc = null;
        try {
            doc = getSAXBuilder().build(file);
        } catch (JDOMException e) {
            throw new XmlConfException("Cannot parse XML document", e);
        } catch (IOException e) {
            throw new XmlConfException("Cannot parse XML document", e);
        }

        return doc;
    }

    private static SAXBuilder getSAXBuilder() {
        return new SAXBuilder(new XMLReaderJDOMFactoryImpl(), null, null);
    }

}