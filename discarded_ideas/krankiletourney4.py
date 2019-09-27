#!/usr/bin/python

import random
import logging
import math
from collections import defaultdict, Counter
from operator import itemgetter

from messages import Upload, Request
from krankiletyrant import KrankileTyrant
from util import even_split
from peer import Peer

class KrankileTourney4(KrankileTyrant):
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

        # We'll put all the things we want here
        requests = []

        # Create a datastructure for tallying up what pieces are the most rare
        piece_counter = Counter()
        for peer in peers: 
            av = set(peer.available_pieces)
            av_needed = av.intersection(np_set)
            piece_counter.update(av_needed)
        
        # Go through each peer and ask for the most rare pieces they have and we want
        for peer in peers:
            available_piece_set = set(peer.available_pieces)
            isect = available_piece_set.intersection(np_set)

            n = min(self.max_requests, len(isect))
            
            lisect = list(isect)

            lisect = sorted(lisect, lambda p1, p2: piece_counter[p1] - piece_counter[p2])
            rarest = [lisect.pop(0)] if len(lisect) else []
            random.shuffle(lisect)

            pieces = rarest + lisect[:n-1]

            for piece_id in pieces:
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

        if len(requests) == 0:
            return []

        n = min(len(requests), 3)

        chosen_requests = random.sample(requests, n)
        chosen = [request.requester_id for request in chosen_requests]
        # Evenly "split" my upload bandwidth among the one chosen requester
        bws = even_split(self.up_bw, len(chosen))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(chosen, bws)]

        return uploads

