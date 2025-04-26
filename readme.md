- [CS30202iitkgp](https://cse.iitkgp.ac.in/~ksrao/cou-dbms-2025.html)
- [cs186berkeley online notes!](https://cs186berkeley.net/notes/)
- [File-Organization-and-Indexing](https://studyglance.in/dbms/display.php?tno=51&topic=File-Organization-and-Indexing-in-DBMS)

<details>
<summary>The total number of block transfers for external
sorting of the relation</summary>


\[
  \boxed{\,\text{Total transfers} 
    = br\;\bigl(2\,\lceil\log_{\,\lfloor M/ bb\rfloor -1}\!(br/M)\rceil \;+\;1\bigr)\;}
\]
where  
- \(br\) = number of blocks in the relation  
- \(M\) = number of buffer blocks available  
- \(bb\) = number of blocks you buffer per run  

---

### 1) **Initial run generation**  
- You read each of the \(br\) blocks into memory once, sort it, and write it back out as runs.  
- **Cost** = \(br\) reads + \(br\) writes = **\(2\,br\)** block transfers.  

---

### 2) **How many runs can we merge at once?**  
- You need one output buffer of size \(bb\), plus one input buffer of size \(bb\) for each run you merge.  
- Total input buffers ≤ \(M - bb\), so  
  \[
    \text{max fan‑in }R
    = \Bigl\lfloor\frac{M - bb}{bb}\Bigr\rfloor
    = \Bigl\lfloor\frac{M}{bb}\Bigr\rfloor \;-\;1.
  \]

---

### 3) **Number of merge passes \(P\)**  
- After run generation you have \(\lceil br/M\rceil\) runs.  
- Each merge pass reduces the run count by a factor of up to \(R\).  
- To get down to 1 final run requires  
  \[
    P \;=\;\Bigl\lceil
       \log_{\,R}\bigl(br/M\bigr)
    \Bigr\rceil
    \;=\;\Bigl\lceil
       \log_{\,\lfloor M/bb\rfloor -1}\!(br/M)
    \Bigr\rceil.
  \]

---

### 4) **Cost of the merge passes**  
- **Passes 1 through \(P-1\)** each read *and* write *all* \(br\) blocks → \(2\,br\) transfers per pass.  
- **Final pass** only needs to read the \(br\) blocks (we don’t count writing the final output back to disk).  

So the merge‐phase transfers are  
\[
  (P-1)\times (2\,br)\;+\;(1)\times(br)
  \;=\;2\,br\,(P-1)\;+\;br.
\]

---

### 5) **Sum everything**  
- Initial run generation: \(2\,br\)  
- Merging: \(2\,br\,(P-1) + br\)  

Total =  
\[
  2\,br \;+\; 2\,br\,(P-1) \;+\; br
  = 2\,br\,P + br
  = br\,\bigl(2\,P + 1\bigr).
\]

Substitute \(P = \bigl\lceil\log_{\,\lfloor M/bb\rfloor -1}(br/M)\bigr\rceil\), and you get

\[
  \boxed{\,br\;\Bigl(2\,\bigl\lceil\log_{\,\lfloor M/bb\rfloor -1}(br/M)\bigr\rceil \;+\;1\Bigr).\,}
\]
</details>
