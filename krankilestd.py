#!/usr/bin/python
import random
import logging

from operator import itemgetter
from itertools import chain
from collections import Counter
from messages import Upload, Request
from util import even_split
from peer import Peer

class KrankileStd(Peer):
    def post_init(self):
        pass
    
    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))
        np_set = set(needed_pieces)  # sets support fast intersection ops.


        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        logging.debug("%s still here. Here are some peers:" % self.id)
        for p in peers:
            logging.debug("id: %s, available pieces: %s" % (p.id, p.available_pieces))

        logging.debug("And look, I have my entire history available too:")
        logging.debug("look at the AgentHistory class in history.py for details")
        logging.debug(str(history))

        requests = []   # We'll put all the things we want here

        # Symmetry breaking is good...
        random.shuffle(needed_pieces)
        
        # Sort peers by id.  This is probably not a useful sort, but other 
        # sorts might be useful
        peers.sort(key=lambda p: p.id)
        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        piece_counter = Counter()
        for peer in peers: 
            av = set(peer.available_pieces)
            av_needed = av.intersection(np_set)
            piece_counter.update(av_needed)
        
            
        for peer in peers:
            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            n = min(self.max_requests, len(isect))
            # More symmetry breaking -- ask for random pieces.
            # This would be the place to try fancier piece-requesting strategies
            # to avoid getting the same thing from multiple peers at a time.
            lisect = list(isect)
            lisect = sorted(lisect, lambda p1,p2: piece_counter[p1] - piece_counter[p2])
            for piece_id in lisect[:n]:
                # aha! The peer has this piece! Request it.
                # which part of the piece do we need next?
                # (must get the next-needed blocks in order)
                start_block = self.pieces[piece_id]
                r = Request(self.id, peer.id, piece_id, start_block)
                requests.append(r)

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """
        
        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.

        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            chosen = []
            bws = []
            return []

        uploader_c = Counter()
        two_last = history.downloads[-2:]
        requester_ids = set(x.requester_id for x in requests)
        for download in chain(*two_last):
            if download.from_id in requester_ids:
                uploader_c.update({download.from_id: download.blocks})    

        chosen = set(x[0] for x in uploader_c.most_common(min(len(requests),3)))
        peer_ids = set(x.requester_id for x in requests).difference(chosen)
        
        if bool(peer_ids):
            chosen.add(random.choice(list(peer_ids)))
        # Evenly "split" my upload bandwidth among the one chosen requester
        bws = even_split(self.up_bw, 4)

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(chosen, bws)]
            
        return uploads
