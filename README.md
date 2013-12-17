bayarea
=======

This is a very basic set of hedonics and location choice models for the Bay Area.

To run the residential rent hedonic and the location choice model for renters, execute the following (and edit the associated JSON files to change the model specification):

```
python run_json.py rrh.json hlcmr.json
```

To run the residential sales hedonic and the location choice model for owners, execute the following (and edit the associated JSON files to change the model specification):

```
python run_json.py rrh.json hlcmr.json
```

To run the non-residential rent hedonic and the location choice model for employers, execute the following (and edit the associated JSON files to change the model specification):

```
python run_json.py nrh.json elcm.json
```
