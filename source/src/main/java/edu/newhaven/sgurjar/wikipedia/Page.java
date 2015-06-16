package edu.newhaven.sgurjar.wikipedia;

import static edu.newhaven.sgurjar.wikipedia.Globals.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/*
 input data

 vertex             pagetype        pagerank        links
 ================================================================
 HMS_Veteran        INITIALIZED     -Infinity       V_and_W_class_destroyer fireship        destroyer       HMS_Prometheus_(1807)   ship_of_the_line        third_rate      Royal_Navy
 HMS_Veteran_(D72)  INITIALIZED     -Infinity       acoustic_mine   knot_(unit)     Hedgehog_(weapon)       World_War_II    boilers nautical_mile   QF_12_pounder_12_cwt_naval_gun  destroyer       John_Brown_&_Company    Pennant_number  Length_between_perpendiculars   Yarrow_boiler   steam_turbines  V_and_W-class_destroyer BL_4.7_inch_/45_naval_gun       Oerlikon_20_mm_cannons  German_invasion_of_Norway       Nanking_Incident        Horsepower      Jack_Broome     China_Station   depth_charge    King_George_V_Dock,_London      Oerlikon_20_mm_cannon   QF_2_pounder_naval_gun  London  Mediterranean_Fleet_(United_Kingdom)    Royal_Navy      Length_overall  British_21_inch_torpedo convoy_SC_42    Atlantic_Fleet_(United_Kingdom)
 HMS_Viceroy_(D91)  INITIALIZED     -Infinity       Woolston,_Hampshire     V_and_W_class_destroyer Mediterranean_Fleet     United_Kingdom  Ship_naming_and_launching       First_Sea_Lord  Keel_laying     KlaipÄda_Region Minesweeper_(ship)      LiepÄja Prime_Minister_(United_Kingdom) Scotland        Port_Edgar      Scapa_Flow      Sweden  Free_City_of_Danzig     convoy  World_War_II    Ship_commissioning      torpedo Helsinki        surrender_of_Japan      Invasion_of_Normandy    Reserve_fleet   SwinemÃ¼nde      England brandy  Copenhagen      Gothenburg      destroyer       Pennant_number  Length_between_perpendiculars   steam_turbines  Tanker_(ship)   Norway  pennant_number  Sicily  Newcastle_upon_Tyne     Battle_honour   Algiers Oslo    Meridon,_Warwickshire   Allies_of_World_War_II  Winston_Churchill       Naval_mine      Sunderland,_Tyne_and_Wear       Stockholm       Trondheim       Latvia  submarine       Ship_breaking   Admiral_of_the_Fleet_(Royal_Navy)       Normandy        Danzig  Grand_Fleet     Water-tube_boiler       Orkney_Islands  Ship_decommissioning    light_cruiser   Denmark North_Sea       Estonia Warship_Week    KlaipÄda        Crown_(headgear)        depth_charge    Malta   Tallinn dockyard        Mediterranean_Sea       Reserve_Fleet_(United_Kingdom)  Flotilla        Warwickshire    Grangemouth     Algeria Riga    surrender_of_Germany    Order_of_the_Star_of_India      Kiel_Canal      Sea_trials      antiaircraft    naval_mine      Royal_Navy      Operation_Husky Length_overall  Hampshire       John_I._Thornycroft_&_Company   German_submarine_U-1274 Atlantic_Fleet_(United_Kingdom) World_War_I     Andrew_Cunningham,_1st_Viscount_Cunningham_of_Hyndhope  Finland Brechin
 HMS_Victor         INITIALIZED     -Infinity       Victor_(disambiguation) Naval_General_Service_Medal_(1847)      List_of_ships_named_HMY_Victoria_and_Albert     Admiralty       sloop-of-war    CSS_Rappahannock_       List_of_ships_named_HMS_Victory Royal_Navy      brig-sloop      Confederate_Navy

 object of this class is used as 'value' so it doesn't need to be comparable
 */

public class Page implements Writable {

  private static final Logger log = Logger.getLogger(Page.class);

  public enum Type {
    INVALID, INITIALIZED, STRUCTURE, MASS, COMPLETED;

    public static Type fromInt(final int n) {
      for (final Type t : Type.values()) {
        if (t.ordinal() == n) {
          return t;
        }
      }
      throw new AssertionError("invalid int '" + n + "'");
    }
  }

  // title is set as key
  public Type type;
  public float rank;
  public Set<String> adjacencyList;

  public Page() {
    clear();
  }

  public void clear() {
    type = Type.INVALID;
    rank = Float.NEGATIVE_INFINITY;
    adjacencyList = null;
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(type.ordinal());
    out.writeFloat(rank);
    final int sz = adjacencyList == null ? 0 : adjacencyList.size();
    out.writeInt(sz);
    if (sz > 0) {
      for (final String a : adjacencyList) {
        out.writeUTF(a);
      }
    }
    //    out.writeUTF(toString());
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    type = Type.fromInt(in.readInt());
    rank = in.readFloat();
    final int sz = in.readInt();
    if (sz > 0) {
      adjacencyList = new HashSet<String>();
      for (int i = 0; i < sz; i++) {
        adjacencyList.add(in.readUTF());
      }
    } else {
      adjacencyList = null;
    }
    //    Page.fromString(in.readUTF(), this);
  }

  @Override
  public String toString() {
    //if (log.isDebugEnabled()) log.debug("toString: <" + type + "," + rank + "," + adjacencyList + ">");

    assertTrue(type != Type.INVALID);

    // toString used by TextOutputFormat to convert this object into string
    final StringBuilder sb = new StringBuilder();

    // type
    sb.append(type); // as string not as ordinal

    // rank
    sb.append('\t').append(rank);

    // links
    if (adjacencyList != null) {
      for (final String a : adjacencyList)
        sb.append('\t').append(a);
    }

    return sb.toString();
  }

  public static Page fromString(final String s, final Page page) {
    if (log.isDebugEnabled()) log.debug("fromString: <" + s + ">");
    // HMS_Veteran INITIALIZED -Infinity V_and_W_class_destroyer fireship destroyer HMS_Prometheus_(1807) ship_of_the_line third_rate Royal_Navy

    final String[] cols = s.split("\t");
    assertTrue(cols.length > 1);

    page.clear();

    try {
      page.type = Page.Type.valueOf(cols[0]);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("'" + s + "'", e);
    }
    assertTrue(page.type != Type.INVALID);

    page.rank = Float.parseFloat(cols[1]);

    for (int i = 2; i < cols.length; i++) {
      if (page.adjacencyList == null) page.adjacencyList = new HashSet<String>();
      page.adjacencyList.add(cols[i]);
    }

    return page;
  }
}
