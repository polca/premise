Exporting sparse matrices
===========================

*premise* can generate a sparse matrix representation of the database(s). This is useful
when no LCA software can be used, or when connections to SQL databases should be avoided.

This is done as follows::

    ndb.write_db_to_matrices()

This creates a set of CSV files:

* `A_matrix.csv`: technosphere exchanges with columns
  `index of activity; index of product; value; uncertainty type; loc; scale; shape; minimum; maximum; negative; flip`.
* `B_matrix.csv`: biosphere exchanges with columns
  `index of activity; index of biosphere flow; value; uncertainty type; loc; scale; shape; minimum; maximum; negative; flip`.
* `A_matrix_index.csv` and `B_matrix_index.csv`: mappings between dataset/flow identifiers and indices.

with *a* being the row index of an activity, *b* being the column index of an activity,
*c* being a natural flow, and *x* being the value exchanged.

For example, the following piece of script calculates the GWP score of all activities in the database:

.. code-block:: python

    """ COLLECT DATA """
    # creates dict of activities <--> indices in A matrix
    A_inds = dict()
    with open("A_matrix_index.csv", 'r') as read_obj:
        csv_reader = reader(read_obj, delimiter=";")
        for row in csv_reader:
            A_inds[(row[0], row[1], row[2], row[3])] = row[4]
    A_inds_rev = {int(v):k for k, v in A_inds.items()}

    # creates dict of bio flow <--> indices in B matrix
    B_inds = dict()
    with open("B_matrix_index.csv", 'r') as read_obj:
        csv_reader = reader(read_obj, delimiter=";")
        for row in csv_reader:
            B_inds[(row[0], row[1], row[2], row[3])] = row[4]
    B_inds_rev = {int(v):k for k, v in B_inds.items()}

    # create a sparse A matrix
    A_coords = np.genfromtxt("A_matrix.csv", delimiter=";", skip_header=1)
    I = A_coords[:, 0].astype(int)
    J = A_coords[:, 1].astype(int)
    A = sparse.csr_matrix((A_coords[:,2], (J, I)))

    # create a sparse B matrix
    B_coords = np.genfromtxt("B_matrix.csv", delimiter=";", skip_header=1)
    I = B_coords[:, 0].astype(int)
    J = B_coords[:, 1].astype(int)
    B = sparse.csr_matrix((B_coords[:,2] *- 1, (I, J)), shape=(A.shape[0], len(B_inds)))

    # a vector with a few GWP CFs
    gwp = np.zeros(B.shape[1])

    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Carbon dioxide, non-fossil, resource correction"]] = -1
    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Hydrogen"]] = 5
    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Carbon dioxide, in air"]] = -1
    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Carbon dioxide, non-fossil"]] = 1
    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Carbon dioxide, fossil"]] = 1
    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Carbon dioxide, from soil or biomass stock"]] = 1
    gwp[[int(B_inds[x]) for x in B_inds if x[0]=="Carbon dioxide, to soil or biomass stock"]] = -1

    l_res = []
    for v in range(0, A.shape[0]):
        f = np.float64(np.zeros(A.shape[0]))
        f[v] = 1
        A_inv = spsolve(A, f)
        C = A_inv * B
        l_res.append((C * gwp).sum())
