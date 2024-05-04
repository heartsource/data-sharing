import com.heartsrc.dto.DatasetDefinition;
import com.heartsrc.processor.ProcessManager;

public class PlayGround {
    public static void main(String[] args) throws Exception {
        ProcessManager mgr = new ProcessManager();
        DatasetDefinition def = new DatasetDefinition();
        def.setGroupByFields( new String[] {"gender","broadcaster"});
        def.setType("brandeffect");
        mgr.run(def);

        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        def = new DatasetDefinition();
        def.setGroupByFields( new String[] {"network","politics"});
        def.setType("broadcaster");
        mgr.run(def);
    }
}
