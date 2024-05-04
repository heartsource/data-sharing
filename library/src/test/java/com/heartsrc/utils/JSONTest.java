package com.heartsrc.utils;

import com.heartsrc.dto.DatasetDefinition;
import com.heartsrc.dto.LinkingDefinition;
import com.heartsrc.dto.RequestObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class JSONTest {

    @Test
    public void testJsonDatasets() throws Exception {
        String json = Jsonizer.toJson(brandEffectDatasetDef()).replaceAll("\\n", " ").replaceAll("\\r", " ");
        String expected =
                "{ \"type\" : \"brandeffect\",    " +
                        "\"keys\" : [ \"202401\", \"202402\" ],    " +
                        "\"measures\" : [ \"Ad Recall\" ],    " +
                        "\"groupByFields\" : [ \"Gender\", \"Brand\" ],    " +
                        "\"filters\" : { \"Age\" : [ \"equals\", \"18-24\" ] } }";
        assertThis(expected, json);
    }
    @Test
    public void testJsonRequestObject() throws Exception {
        RequestObject obj = new RequestObject();
        obj.setDatasets(new DatasetDefinition[] {brandEffectDatasetDef(), broadcasterDatasetDef()});
        String json = Jsonizer.toJson(obj).replaceAll("\\n", " ").replaceAll("\\r", " ");
        String expected =
                "{\"datasets\":[{\"type\":\"brandeffect\",\"keys\":[\"202401\",\"202402\"]," +
                        "\"measures\":[\"AdRecall\"]," +
                        "\"groupByFields\":[\"Gender\",\"Brand\"]," +
                        "\"filters\":{\"Age\":[\"equals\",\"18-24\"]}}," +
                        "{\"type\":\"broadcaster\",\"keys\":[\"2024Q1\"]," +
                        "\"measures\":[\"PoliticalScore\"]," +
                        "\"groupByFields\":[\"gender\",\"age_group\"]," +
                        "\"filters\":{\"age_group\":[\"equals\",\"18-24\"]}}]," +
                        "\"linking\":null}";
        assertThis(expected, json);
    }
    @Test
    public void testJsonJoin() throws Exception {
        String json = Jsonizer.toJson(link1()).replaceAll("\\n", " ").replaceAll("\\r", " ");
        String expected = "{\"fromDatasetType\":\"brandeffect\",\"toDatasetType\":\"broadcaster\",\"joins\":[[\"Age\",\"age_group\"]]}";
        assertThis(expected, json);
    }

    private static void assertThis(String expected, String json) {
        Assertions.assertEquals(expected.replaceAll(" ",""), json.replaceAll(" ",""));
    }

    private LinkingDefinition link1() {
        LinkingDefinition link = new LinkingDefinition();
        link.setFromDatasetType("brandeffect");
        link.setToDatasetType("broadcaster");
        link.setJoins(new String[][] {{"Age","age_group"}});
        return link;
    }

    private DatasetDefinition brandEffectDatasetDef() {
        DatasetDefinition def = new DatasetDefinition();
        def.setType("brandeffect");
        def.setKeys(new String[]{"202401","202402"});
        Map<String, String[]> filters = new HashMap<>();
        filters.put("Age", new String[] {"equals","18-24"});
        def.setFilters(filters);
        def.setGroupByFields(new String[] {"Gender","Brand"});
        def.setMeasures(new String[] {"Ad Recall"});
        return def;
    }
    private DatasetDefinition broadcasterDatasetDef() {
        DatasetDefinition def = new DatasetDefinition();
        def.setType("broadcaster");
        def.setKeys(new String[]{"2024Q1"});
        Map<String, String[]> filters = new HashMap<>();
        filters.put("age_group", new String[] {"equals","18-24"});
        def.setFilters(filters);
        def.setGroupByFields(new String[] {"gender", "age_group"});
        def.setMeasures(new String[] {"Political Score"});
        return def;
    }
}
