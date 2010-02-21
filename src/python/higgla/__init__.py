if __name__ == "__main__":
    import json

    from higgla.client import *
    session = higgla.client.Session("mybase")

    print "LOOKUP mke AND foobar RESULTS"
    try:
        print str(session.get("mke", "mydoc2"))
    except HigglaException, e:
        print e
    print ""

    print "STORE RESULTS"
    box = session.prepare_box("mke", 0, "firstname",
                              firstname="Mikkel", lastname="Kamstrup")
    box["address"] = "57 Mount Pleasant Street"
    try:
        print str(json.dumps(session.store(box), indent=2))
    except HigglaException, e:
        print "Error: %s" % e
    print ""

    print "QUERY RESULTS"
    q = session.prepare_query(firstname="mikkel")
    print str(json.dumps(session.send_query(myquery=q), indent=2))
    print ""