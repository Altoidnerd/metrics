
# Models:
1. Preict engagmenet based on user attributes and events.
* Useful to:
    - Discover engagement drivers
    
$f:  \{users\} \longrightarrow engagement$

$f(X(user)) = y$

2. Cluster users
* Useful to: 
  - Discover user classes
  - Identify features of known classes of users
  
$f: \{users\} \longrightarrow \{user class\}$

3. Predict utilization of (engagement with) nft collection based on nftcollection attributes.
* Useful to:
  - Discover user product relationships
  - Discover engagement drivers
  
$f: \{nftcollection\} \longrightarrow engagement$


# Model Features
Need to determine useful features to feed into models.


### User Engagement Metrics
frequency and depth of use
* screen time
* app features utilized
    - video watch
    - comment
    - video create
* use frequency
* retention
* behaviors


X(mongo):
### User Properties
* web3 or not
    - verified nft or not
* notifications enabled
* ownership
    - how many verified nfts, which ones ( experiences.certified: bool )
    - how many experiences
* device model
    - device's performance w.r.t. feature
    - device characteristics (AR performance: double, supported: bool)


    
X(events)
### User Behviors
* video created
* how acquired
    - apple
    - regular
    - wallet connect

* whether in discord: discord dataset
    - wallet_connect, wallet addr in discord data?
    - discord roles: device fingerprinting
        > device model / OS (android vs ios)

* map use / interaction
* what user did (events) \[before web3 (clustering?)\]
1.  use of social feed
2.  using camera
3.  event: feature_session__start, prop: feature_session_type
4.  create/publish video
    - whether checked social graph event: collectible_value__social, prop: experience_id, experience_name, is_nft, ...
5.  whether hit discover page -> event: feature_session__start, prop: feature_session_type OR event: 
6.  engagement from others induced by created comment
    - likes
    - views
    - comments
    - followers
7.  notificationEnabled, wallet connected, ... permissivity other
8.  unlocked experiences [0,0,0,1,0,0,1,0,1,1,0 ...]  
    - free vs restricted (experiences.restricted: bool)
    - unlockedexperiences.type 
    - find out more 
9.  whether bought / earned something (ownership of an experience)
    - purchased with tokens / event: ?
    - ~~gotten with blue coin~~
10. chameleon game
    - event: homefeed__chamelion
11. anonymous users (need tester compile flag?)
    - loss of collectible


# NFT collections
* Need NFT collection properties from team
* 
* 
* 
* 
<!-- 
# User Clustering
* cluster by event space
* set up debug mode for event generation -->