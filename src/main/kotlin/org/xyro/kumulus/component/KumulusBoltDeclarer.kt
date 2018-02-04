package org.xyro.kumulus.component

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.topology.InputDeclarer
import org.apache.storm.tuple.Fields

class KumulusBoltDeclarer() : InputDeclarer<KumulusBoltDeclarer> {
    override fun allGrouping(componentId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun allGrouping(componentId: String?, streamId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun shuffleGrouping(componentId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun shuffleGrouping(componentId: String?, streamId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun noneGrouping(componentId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun noneGrouping(componentId: String?, streamId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun partialKeyGrouping(componentId: String?, fields: Fields?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun partialKeyGrouping(componentId: String?, streamId: String?, fields: Fields?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun localOrShuffleGrouping(componentId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun localOrShuffleGrouping(componentId: String?, streamId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun fieldsGrouping(componentId: String?, fields: Fields?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun fieldsGrouping(componentId: String?, streamId: String?, fields: Fields?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun grouping(id: GlobalStreamId?, grouping: Grouping?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun customGrouping(componentId: String?, grouping: CustomStreamGrouping?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun customGrouping(componentId: String?, streamId: String?, grouping: CustomStreamGrouping?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun globalGrouping(componentId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun globalGrouping(componentId: String?, streamId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun directGrouping(componentId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun directGrouping(componentId: String?, streamId: String?): KumulusBoltDeclarer {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}