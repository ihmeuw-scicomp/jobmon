import React from 'react'
import { Modal, Button } from 'react-bootstrap';


export default function CustomModal({headerContent, bodyContent, showModal, setShowModal, className = ''}) {

    return (
        <Modal
            size="xl"
            aria-labelledby="contained-modal-title-vcenter"
            centered
            show={showModal}
            onHide={() => { setShowModal(false) }}
            className={className}
        >
            <Modal.Header className='bg-grey px-4 pt-4'>
                <Modal.Title id="contained-modal-title-vcenter">
                    {headerContent}
                </Modal.Title>
            </Modal.Header>
            <Modal.Body className='px-4'>
                <pre>
                    {bodyContent}
                </pre>
            </Modal.Body>
            <Modal.Footer className='px-4'>
                <Button variant="dark" onClick={() => { setShowModal(false) }}>
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
  )
}
