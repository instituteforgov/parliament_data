# Parliament data

## Members API
Example: https://members-api.parliament.uk/api/Members/1581.

Several options exist for parliamentarians' names:
- `nameDisplayAs` Name, generally only with a short title. E.g. 'Ms Diane Abbott'
- `nameListAs`: Name ordered for alphaetical sort. E.g. 'Abbott, Ms Diane'
- `nameFullTitle`: Name with full title. E.g. 'Rt Hon Diane Abbott MP'
- `nameAddressAs`: Suggested name for correspondence. E.g. 'Ms Abbott'

`nameDisplayAs` is the name extracted for our use.

Some, records have `statusStartDate` before someone's house membership begins, e.g. Lord Chartres: https://members-api.parliament.uk/api/Members/1946.

## Members history API

Example: https://members-api.parliament.uk/api/Members/History/?ids=1581

### Name history
Name options as per members API.

### Party history
For **MPs**:
- A single record for periods in a party before the May 2015 election (note the contrast here with house membership history);
- One or more record per parliament from that point onwards;

In all cases:
- `startDate` is the date of the election at which someone entered parliament;
- `endDate` is:
    - the date of the election at which someone left parliament for elections before May 2015;
    - the date the pre-election period starts for the May 2015 election and subsequent elections.

For **peers**, a single record for periods in a party, with elections having no effect.

Where someone was an **MP** and became a **peer**, separate records for each.

Some, generally historic, periods in parliament don't have associated party history details e.g. Jeffrey Archer's time in the Commons in the 1960s/1970s: https://members-api.parliament.uk/api/Members/History/?ids=1612.

### House membership history
For **MPs**, one record per parliament (including those before May 2015 - note the contrast here with party history), where:

- `startDate` is the date of the election at which someone entered parliament;
- `endDate` is:
    - the date of the election at which someone left parliament for elections before May 2015;
    - the date the pre-election period starts for the May 2015 election and subsequent elections.

For MPs `membershipFrom` values are constituency names.

For **peers**, a single record.

For peers, `membershipFrom` values are the type of peerage held (e.g. life peer, excepted hereditary). `membershipFromID` values up to and including 10 used for these `membershipFrom` values.

For some excepted hereditary peers, `membershipStartDate` appears to be missing e.g. Earl Cathcart's time as an excepted hereditary peers from March 2007: https://members-api.parliament.uk/api/Members/History/?ids=2463 (compare vs https://members-api.parliament.uk/api/Members/2463).