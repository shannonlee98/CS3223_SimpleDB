package simpledb.parse;

public class Pair<Left, Right> {
    private Left left;
    private Right right;

    public Pair(Left left, Right right){
        this.left = left;
        this.right = right;
    }

    public Left getL(){ return left; }

    public Right getR(){ return right; }

    public void setL(Left left){ this.left = left; }

    public void setR(Right right){ this.right = right; }
}