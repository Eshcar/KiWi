package trees.lockbased.lockremovalutils;

import java.util.HashMap;
import java.util.Map.Entry;

public final class LockSet{
	private SpinHeapReentrant e1;
	private SpinHeapReentrant e2;
	private SpinHeapReentrant e3;
	private SpinHeapReentrant e4;
	private SpinHeapReentrant e5;
	private SpinHeapReentrant e6;
	private SpinHeapReentrant e7;
	private SpinHeapReentrant e8;
	private SpinHeapReentrant e9;
	private int max;
	
	public LockSet(){
	}

	public LockSet(int size){
	}
	
	public void clear(){
//System.out.println("clear");	   	   	
       int oldMax = max;
	   max = 0;
	   if(e1!=null) e1=null; if(oldMax<=1) return ;
	   if(e2!=null) e2=null; if(oldMax<=2) return ;
	   if(e3!=null) e3=null; if(oldMax<=3) return ;
	   if(e4!=null) e4=null; if(oldMax<=4) return ;
	   if(e5!=null) e5=null; if(oldMax<=5) return ;
	   if(e6!=null) e6=null; if(oldMax<=6) return ;
	   if(e7!=null) e7=null; if(oldMax<=7) return ;
	   if(e8!=null) e8=null; if(oldMax<=8) return ;
	   if(e9!=null) e9=null;
	}
	
	public void add(SpinHeapReentrant node){
   	   assert(node!=null);
//System.out.println("add");	   	   	   
	   if(e1==null) { e1=node; if(max<1) max=1; return; }
	   if(e2==null) { e2=node; if(max<2) max=2; return; }
	   if(e3==null) { e3=node; if(max<3) max=3; return; }
	   if(e4==null) { e4=node; if(max<4) max=4; return; }
	   if(e5==null) { e5=node; if(max<5) max=5; return; }
	   if(e6==null) { e6=node; if(max<6) max=6; return; }
	   if(e7==null) { e7=node; if(max<7) max=7; return; }
	   if(e8==null) { e8=node; if(max<8) max=8; return; }
	   if(e9==null) { e9=node; if(max<9) max=9; return; }	
	   System.out.println("bbbbb");	   
	   assert(false);
	}
	
	public void remove(SpinHeapReentrant node){
   	   assert(node!=null);
//System.out.println("remove");	   	   
	   if(e1==node) { e1=null; return; } if(max<=1) return ;
	   if(e2==node) { e2=null; return; } if(max<=2) return ;
	   if(e3==node) { e3=null; return; } if(max<=3) return ;
	   if(e4==node) { e4=null; return; } if(max<=4) return ;
	   if(e5==node) { e5=null; return; } if(max<=5) return ;
	   if(e6==node) { e6=null; return; } if(max<=6) return ;
	   if(e7==node) { e7=null; return; } if(max<=7) return ;
	   if(e8==node) { e8=null; return; } if(max<=8) return ;
	   if(e9==node) { e9=null; return; } 
System.out.println("aaaa");	   
	   assert(false);
	}
	
	
	public boolean tryLockAll(Thread self){
//System.out.println("tryLockAll");	   	   	
	    boolean failed = false;
		int c = 0;
	    if(!failed && e1!=null) { if (!e1.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e2!=null) { if (!e2.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e3!=null) { if (!e3.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e4!=null) { if (!e4.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e5!=null) { if (!e5.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e6!=null) { if (!e6.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e7!=null) { if (!e7.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e8!=null) { if (!e8.tryAcquire(self)) failed=true; else c++; }
	    if(!failed && e9!=null) { if (!e9.tryAcquire(self)) failed=true; else c++; }
		
		if(failed) { //System.out.println("failed in tryLockAll ");
		   if(c>0 && e1!=null) {e1.release(); e1=null; c--;} 
		   if(c>0 && e2!=null) {e2.release(); e2=null; c--;} 
		   if(c>0 && e3!=null) {e3.release(); e3=null; c--;} 
		   if(c>0 && e4!=null) {e4.release(); e4=null; c--;} 
		   if(c>0 && e5!=null) {e5.release(); e5=null; c--;} 
		   if(c>0 && e6!=null) {e6.release(); e6=null; c--;} 
		   if(c>0 && e7!=null) {e7.release(); e7=null; c--;} 
		   if(c>0 && e8!=null) {e8.release(); e8=null; c--;} 
		   if(c>0 && e9!=null) {e9.release(); e9=null; c--;} 
		}
		return (!failed);
	}

	public void releaseAll() {
//System.out.println("releaseAll");	   		
		   if(e1!=null) {e1.release(); e1=null;}
		   if(e2!=null) {e2.release(); e2=null;}
		   if(e3!=null) {e3.release(); e3=null;}
		   if(e4!=null) {e4.release(); e4=null;}
		   if(e5!=null) {e5.release(); e5=null;}
		   if(e6!=null) {e6.release(); e6=null;}
		   if(e7!=null) {e7.release(); e7=null;}
		   if(e8!=null) {e8.release(); e8=null;}
		   if(e9!=null) {e9.release(); e9=null;}	
	}
	
	//for debug
	public int getCount() {
	assert(false);
return 0;
	}
	
}
