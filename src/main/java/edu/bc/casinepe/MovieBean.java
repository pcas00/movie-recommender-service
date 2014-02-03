package edu.bc.casinepe;

public class MovieBean {

    private int id;
    private String title;
    private double rating;

    public MovieBean() {}

    public MovieBean(int id, String title, double rating) {
        this.id = id;
        this.title = title;
        this.rating = rating;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public int getId() {
        return this.id;
    }

    public String getTitle() {
        return this.title;
    }

    public double getRating() {
        return this.rating;
    }


}