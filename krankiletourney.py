#!/usr/bin/python

import random
from messages import Upload, Request
from util import even_split
from peer import Peer


class KrankileTourney(Peer):

    def post_init(self):
        # For making it easier to change parameters
        self.slots = 2

    def piece_needed(self, piece_id):
        # If we have less blocks for a particular piece than is set as the piece size we need that piece
        return self.pieces[piece_id] < self.conf.blocks_per_piece

    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see
        returns: a list of Request() objects
        This will be called after update_pieces() with the most recent state.
        """

        # Find the pieces we need
        needed_pieces = list(
            set(filter(self.piece_needed, range(len(self.pieces)))))

        # Initialize an array to hold the request objects we are going to send
        requests = []

        # Iterate through all peers to make requests for their pieces
        for peer in peers:

            # Start by finding all pieces we need and this peer also has
            # Also find the maximium amount of requests to send to this peer
            available_pieces = set(peer.available_pieces)
            needed_and_available = available_pieces.intersection(needed_pieces)
            max_requests = min(self.max_requests, len(needed_and_available))

            # Iterate through a random sample of the pieces we identified to break symmetry
            for piece_id in random.sample(needed_and_available, max_requests):
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
        # No requests received -> just terminate
        if len(requests) == 0:
            return []

        # Fin the amount of slots we want to allocate this round
        n_slots = min(self.slots, len(requests))

        # Choose n peers completely at random from the set of peers that requested pieces
        # This again break symmetry and makes us somewhat strategy proof (more on that in the writeup)
        # Also, this makes the likelihood of being reciprocated very large
        # since we give away so much bw to a few peers
        chosen_requests = random.sample(requests, n_slots)
        chosen = [request.requester_id for request in chosen_requests]

        # Distribute the bw among the chosen peers
        bws = even_split(self.up_bw, len(chosen))

        # Create the upload objects out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
