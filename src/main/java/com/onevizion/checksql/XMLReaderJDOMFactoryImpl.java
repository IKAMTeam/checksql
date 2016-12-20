package com.onevizion.checksql;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.jdom2.JDOMException;
import org.jdom2.input.sax.XMLReaderJDOMFactory;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class XMLReaderJDOMFactoryImpl implements XMLReaderJDOMFactory {

    private final SAXParserFactory jaxpfactory;

    private final boolean validates;

    public XMLReaderJDOMFactoryImpl() {
        SAXParserFactory fac = SAXParserFactory.newInstance();
        fac.setNamespaceAware(true);
        fac.setValidating(false);
        jaxpfactory = fac;
        validates = false;
    }

    @Override
    public XMLReader createXMLReader() throws JDOMException {
        if (jaxpfactory == null) {
            throw new JDOMException("It was not possible to configure a " +
                    "suitable XMLReader to support " + this);
        }
        try {
            return jaxpfactory.newSAXParser().getXMLReader();
        } catch (SAXException e) {
            throw new JDOMException(
                    "Unable to create a new XMLReader instance", e);
        } catch (ParserConfigurationException e) {
            throw new JDOMException(
                    "Unable to create a new XMLReader instance", e);
        }
    }

    @Override
    public boolean isValidating() {
        return validates;
    }

}