package edu.bc.casinepe.core;

import org.apache.mahout.cf.taste.recommender.IDRescorer;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by petercasinelli on 4/2/14.
 */
public class MultipleIDRescorer implements IDRescorer {
    List<IDRescorer> idRescorerList = new LinkedList<IDRescorer>();

    public MultipleIDRescorer() {}

    public MultipleIDRescorer(IDRescorer idRescorer) {
        if (idRescorer != null) {
            addIdRescorer(idRescorer);
        }
    }

    public void addIdRescorer(IDRescorer idRescorer) {
        if (idRescorer != null) {
            idRescorerList.add(idRescorer);
        }    }

    @Override
    public double rescore(long id, double originalScore) {
        double newScore = originalScore;

        for (IDRescorer idRescorer : idRescorerList) {
            newScore = idRescorer.rescore(id, newScore);
        }
        return newScore;
    }

    @Override
    public boolean isFiltered(long id) {
        return false;
    }
}
