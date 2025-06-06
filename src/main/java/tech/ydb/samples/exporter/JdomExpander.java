package tech.ydb.samples.exporter;

import java.util.HashMap;
import java.util.Properties;
import org.apache.commons.text.StringSubstitutor;
import org.jdom2.Attribute;
import org.jdom2.CDATA;
import org.jdom2.Content;
import org.jdom2.Element;
import org.jdom2.Text;

/**
 *
 * @author mzinal
 */
public class JdomExpander {

    public static StringSubstitutor makeSubstitutor(Properties props) {
        final HashMap<String, String> m = new HashMap<>();
        props.forEach((k, v) -> m.put(k.toString(), v.toString()));
        return new StringSubstitutor(m);
    }
    
    public static String makeText(StringSubstitutor ss, String input) {
        String value;
        if (input.contains(StringSubstitutor.DEFAULT_VAR_START)) {
            value = ss.replace(input);
        } else {
            value = input;
        }
        return value;
    }

    public static Element expand(Element e, Properties props) {
        if (props==null || props.isEmpty()) {
            return e;
        }
        return expand(e, makeSubstitutor(props));
    }

    public static Element expand(Element e1, StringSubstitutor ss) {
        Element e2 = new Element(e1.getName());
        for (Attribute attr : e1.getAttributes()) {
            e2.setAttribute(attr.getName(), makeText(ss, attr.getValue()));
        }
        for (Content c1 : e1.getContent()) {
            Content c2;
            if (c1 instanceof Element xe1) {
                c2 = expand(xe1, ss);
            } else if (c1 instanceof CDATA xe1) {
                c2 = new CDATA(makeText(ss, xe1.getValue()));
            } else if (c1 instanceof Text xe1) {
                c2 = new Text(makeText(ss, xe1.getValue()));
            } else {
                continue;
            }
            e2.addContent(c2);
        }
        return e2;
    }

}
