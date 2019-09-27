#!/usr/bin/python
import random

from collections import Counter
from itertools import chain
from messages import Upload, Request
from util import even_split
from peer import Peer


class KrankileStd(Peer):
    def post_init(self):
        # To generalise the agent somewhat
        self.normal_slots = 3
        self.optimistic_unchoke_interval = 3

        # To hold the peer that is currently being optimistically unchoked
        self.optimistic_unchoke = None

    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        # Find all the pieces we ned
        def needed(i): return self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = set(filter(needed, range(len(self.pieces))))

        # Create a datastructure for tallying up what pieces are the most rare
        piece_counter = Counter()
        for peer in peers:
            av = set(peer.available_pieces)
            av_needed = av.intersection(needed_pieces)
            piece_counter.update(av_needed)

        # List to keep all requests we want to send out
        requests = []

        # Go through each peer and ask for the n most rare pieces they have and we want
        for peer in peers:
            available_piece_set = set(peer.available_pieces)
            # We can only ask for pieces we (1) need and (2) the peer actually has
            needed_and_available = available_piece_set.intersection(
                needed_pieces)
            # Check how many pieces we can maximally request
            num_pieces = min(self.max_requests, len(needed_and_available))

            # Sort the pieces based on how many peers actually have the piece, rarest first
            rarest_first = sorted(
                list(needed_and_available), lambda p1, p2: piece_counter[p1] - piece_counter[p2])
            for piece_id in rarest_first[:num_pieces]:
                start_block = self.pieces[piece_id]
                request = Request(self.id, peer.id, piece_id, start_block)
                requests.append(request)

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        # No need to waste compute if there are no requests
        if len(requests) == 0:
            return []

        # Initialize a counter to keep track of how much the agent downloaded from all other
        # peers the last two rounds
        uploader_c = Counter()
        two_last = history.downloads[-2:]
        requester_ids = set(x.requester_id for x in requests)

        # Iterate over all download objects that we recieved the previous two rounds
        for download in chain(*two_last):
            # Only count the downloads that are from a peer that actually requested a piece from us
            # requster_ids is a set so this operation is luckily O(1)
            if download.from_id in requester_ids:
                uploader_c.update({download.from_id: download.blocks})

        # Choose the n peers who uploaded most to us as the ones we are going to reciprocate
        # Choose n to be the smaller of how many normal slots there are and how many requesters there are
        chosen = set(x[0] for x in uploader_c.most_common(
            min(len(requester_ids), self.normal_slots)))
        peer_ids = set(x.requester_id for x in requests).difference(chosen)

        # If there still are peers left to unchoke
        # Select a peer to optimistically unchoke if we either do not currently have unchoked anyone
        # or if 3 rounds have passed
        if bool(peer_ids) and (len(history.downloads) % self.optimistic_unchoke_interval == 0 or not self.optimistic_unchoke):
            unchoke = random.choice(list(peer_ids))
            self.optimistic_unchoke = unchoke

        # Add the optimistically unchoked peer to the set
        if self.optimistic_unchoke:
            chosen.add(self.optimistic_unchoke)

        # No need to go any further if no peers were chosen
        if len(chosen) < 1:
            return []

        # Distribute as evenly as possible the bw across the chosen peer(s)
        bws = even_split(self.up_bw, len(chosen))

        # Create upload objects for the peer(s) with their respective allocated bandwidth(s)
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
